/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, ListenerName, LoginType, Mode, Selectable, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
  *
  * 一共三种线程。一个Acceptor线程负责处理新连接请求，会有N个Processor线程，每个都有自己的Selector，负责从socket中读取请求和将返回结果写回。然后会有M个Handler线程，负责处理请求，并且将结果返回给Processor。
  * 将Acceptor和Processor线程分开的目的是为了避免读写频繁影响新连接的接收
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  private val endpoints = config.listeners.map(l => l.listenerName -> l).toMap
  private val numProcessorThreads = config.numNetworkThreads
  private val maxQueuedRequests = config.queuedMaxRequests
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)

  /**
    * 此集合中包含所有Endpoint对应的Processors线程，如图4-7所示。
    */
  private val processors = new Array[Processor](totalProcessorThreads)

  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()

  /**
    * ConnectionQuotas类型的对象。在ConnectionQuotas中，提供了控制每个IP上的最大连接数的功能。
    * 底层通过一个Map对象，记录每个IP地址上建立的连接数，创建新Connect时与maxConnectionsPerIpOverrides指定的最大值（或maxConnectionsPerIp）进行比较，
    * 若超出限制，则报错。因为有多个Acceptor线程并发访问底层的Map对象，则需要synchronized进行同步。
    *
    */
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   */
  def startup() {
    this.synchronized {

      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      val sendBufferSize = config.socketSendBufferBytes
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      config.listeners.foreach { endpoint =>

        /**
          * Endpoint集合。一般的服务器都有多块网卡，可以配置多个IP，Kafka可以同时监听多个端口。
          * Endpoint类中封装了需要监听的host、port及使用的网络协议。每个Endpoint都会创建一个对应的Acceptor对象。
          *
          */
        val listenerName = endpoint.listenerName
        val securityProtocol = endpoint.securityProtocol

        val processorEndIndex = processorBeginIndex + numProcessorThreads

        /**
          * endpoint与Processor对象的集合
          *
          */
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, listenerName, securityProtocol)

        /**
          * 1、每个endpoint对应一个 Acceptor
          * 2、Acceptor创建过程中启动了Processor线程：每个Acceptor线程对应多个Processor
          */
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId, processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)


        /**
          * acceptor是一个runnable，此处是启动一个线程
          */
        Utils.newThread(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor, false).start()

        /**
          * 主线程阻塞，等待Acceptor线程启动完成
          */
        acceptor.awaitStartup()

        /**
          * 重置 processorBeginIndex
          */
        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map { metricName =>
          Option(metrics.metric(metricName)).fold(0.0)(_.value)
        }.sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  /**
    * 监听器：当Handler线程向某个responseQueue中写入数据时，会唤醒对应的Processor线程进行处理
    */
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config.values,
      metrics,
      credentialProvider
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String): Unit = {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  /**
    * 创建选择器
    */
  private val nioSelector = NSelector.open()

  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  this.synchronized {
    /**
      * processor是一个Runnable实现类，遍历processors的过程，就是创建和启动新线程的过程
      */
    processors.foreach { processor =>
      Utils.newThread(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}", processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   *
   * Acceptor只负责接收新连接，并采用round-robin的方式交给各个Processor
   */
  def run() {
    /**
      * 将 Channel 注册到选择器中
      * SelectionKey.OP_ACCEPT表示channel的状态，有四种状态，
      */
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)

    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          /**
            * 最多等待500毫秒的时间，看是否有socket过来
            */
          val ready = nioSelector.select(500)

          if (ready > 0) {

            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()

            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable)
                /**
                  * Acceptor接收配置socket并传给processor
                  */
                  accept(key, processors(currentProcessor))

                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")
                // round robin to the next processor thread
                currentProcessor = (currentProcessor + 1) % processors.length

              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }

            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)

    /**
      * 创建一个 ServerSocketChannel
      */
    val serverChannel = ServerSocketChannel.open()

    serverChannel.configureBlocking(false)

    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)

      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {

      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))

    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }

    serverChannel

  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {

    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()

    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)

      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)

      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      /**
        * Acceptor传过来的新socket放在了一个ConcorrentLinkedQueue中
        */
      processor.accept(socketChannel)

    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }

  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 *
 * Processor主要用于完成读取请求和写回响应的操作，Processor不参与具体业务逻辑的处理。
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel, // Processor与Handler线程之间传递数据的队列。
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics,
                               credentialProvider: CredentialProvider) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  /**
    * newConnections队列由Acceptor线程和Processor线程并发操作，所以选择线程安全的ConcurrentLinkedQueue。
    */
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()

  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {

      def value = {

        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags))).fold(0.0)(_.value)

      }

    },

    metricTags.asScala

  )

  /**
    * kafka自己封装的内容：
    */
  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.serverChannelBuilder(securityProtocol, channelConfigs, credentialProvider.credentialCache))

  override def run() {

    /**
      * 首先调用startupComplete()方法，标识Processor的初始化流程已经结束，唤醒阻塞等待此Processor初始化完成的线程。
      */
    startupComplete()

    while (isRunning) {
      try {

        /**
          * 处理队列中的新连接：congiureNewConnections()负责获取ip端口号等信息然后注册到Processor自己的selector上。这个selector是Kafka封装了一层的KSelector
          */
        // setup any new connections that have been queued up
        configureNewConnections()

        // register any new responses for writing
        processNewResponses()

        /**
          * 带timeout的poll一次
          */
        poll()

        /**
          * 接收Request
          */
        processCompletedReceives()

        /**
          * 处理已经发送成功的Response
          */
        processCompletedSends()

        /**
          * 处理已经断开的连接
          */
        processDisconnected()


      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {

    var curr = requestChannel.receiveResponse(id)

    while (curr != null) {

      try {

        curr.responseAction match {

          /**
            * 根据Channel的不同动作，做不同的处理
            */
          case RequestChannel.NoOpAction =>

            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer

            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)

            val channelId = curr.request.connectionId

            if (selector.channel(channelId) != null || selector.closingChannel(channelId) != null)
                selector.unmute(channelId)

          case RequestChannel.SendAction =>
            sendResponse(curr)

          case RequestChannel.CloseConnectionAction =>

            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)

        }

      } finally {

        curr = requestChannel.receiveResponse(id)

      }

    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {

      /**
        * 如果Response是SendAction类型，表示该Response需要发送给客户端，则查找对应的KafkaChannel，
        * 为其注册OP_WRITE事件，并将KafkaChannel.send字段指向待发送的Response对象。
        * 同时还会将Response从responseQueue队列中移出，放入inflightResponses中。
        * 如果读者关心OP_WRITE事件的取消时机，可以回顾KafkaChannel.send()方法，即发送完一个完整的响应后，会取消此连接注册的OP_WRITE事件。
        *
        */
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)

    }
  }

  private def poll() {

    try selector.poll(300)

    catch {
      case e @ (_: IllegalStateException | _: IOException) =>

        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())

        shutdownComplete()

        throw e

    }

  }

  /**
    * Selector在接收到请求后，将数据放到一个List中，Processor取出后put到requestChannel的requestQueue中
    */
  private def processCompletedReceives() {

    selector.completedReceives.asScala.foreach { receive =>

      try {
        val openChannel = selector.channel(receive.source)

        val session = {
          // Only methods that are safe to call on a disconnected channel should be invoked on 'channel'.
          val channel = if (openChannel != null) openChannel else selector.closingChannel(receive.source)
          RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName), channel.socketAddress)
        }

        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session,
          buffer = receive.payload, startTimeMs = time.milliseconds, listenerName = listenerName,
          securityProtocol = securityProtocol)
        requestChannel.sendRequest(req)

        selector.mute(receive.source)

      } catch {

        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)

      }

    }

  }

  /**
    * 调用processCompletedSends()方法处理KSelector.completedSends队列。
    * 首先，将inflightResponses中保存的对应Response删除。之后，为对应连接重新注册OP_READ事件，允许从该连接读取数据。
    */
  private def processCompletedSends() {

    selector.completedSends.asScala.foreach { send =>
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()
      selector.unmute(send.destination)
    }

  }

  private def processDisconnected() {
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   *
   * 处理newConnections队列中的新建SocketChannel。
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {

        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")

        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString

        /**
          * 注册读事件：
          * 处理newConnections队列中的新建SocketChannel。队列中的每个SocketChannel都要在nioSelector上注册OP_READ事件。
          * 这里有个细节，SocketChannel会被封装成KafkaChannel，并附加(attach)到SelectionKey上，所以后面触发OP_READ事件时，
          * 从SelectionKey上获取的是KafkaChannel类型的对象。KSelector以及KafkaChannel的相关介绍请读者参考第2章。
          *
          */
        selector.register(connectionId, channel)

      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          val remoteAddress = channel.getRemoteAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
