package messaging

import com.rabbitmq.client.*
import java.io.IOException
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException


class RpcClient: AutoCloseable {
  //어떤 큐로부터 메시지를 받을지를 정의한다.
  private val connection: Connection
  private val channel: Channel
  private val requestQueueName = "rpc_queue"

  init {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    connection = factory.newConnection()
    channel = connection.createChannel()
  }

  //RPC 요청을 보내고 응답을 받는다.
  fun call(message: String): String {
    // 응답과 요청을 매핑하기 위한 고유한 ID를 생성한다.
    val corrId: String = UUID.randomUUID().toString()

    // 응답을 받기 위한 독자적 큐를 생성한다.
    val replyQueueName = channel.queueDeclare().queue

    val props = AMQP.BasicProperties.Builder()
      // 응답을 받기 위한 고유한 ID를 지정한다.
      .correlationId(corrId)
      // 응답을 받을 큐를 지정한다.
      .replyTo(replyQueueName)
      .build()

    // 응답을 받기 위한 ID와 큐를 지정하여 rpc_queue를 통해 서버에 요청 메시지를 발행한다.
    channel.basicPublish("", requestQueueName, props, message.toByteArray())

    //분리된 쓰레드에서 응답을 받으므로 CompletableFuture를 사용한다.
    val response = CompletableFuture<String>()

    val consumerTag = channel.basicConsume(replyQueueName, true,
      { _, delivery: Delivery ->
        // 응답 메시지의 고유한 ID와 요청 메시지의 고유한 ID가 일치하는지 확인한다.
        if (delivery.properties.correlationId == corrId) {
          response.complete(String(delivery.body))
        }
      }
    ) { _ -> }

    val result = response.get()
    channel.basicCancel(consumerTag)
    return result
  }

  override fun close() {
    connection.close()
  }
}

fun main() {
  RpcClient().use { fibonacciRpc ->
    for (i in 0..31) {
      val message = i.toString()
      println(" [x] Requesting fib($message)")
      val response: String = fibonacciRpc.call(message)
      println(" [.] Got '$response'")
    }
  }
}
