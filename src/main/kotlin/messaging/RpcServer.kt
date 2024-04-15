package messaging

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.MessageProperties

class RpcServer {
  private val RPC_QUEUE_NAME = "rpc_queue"

  private fun fib(n: Int): Int {
    if (n == 0) return 0
    if (n == 1) return 1
    return fib(n - 1) + fib(n - 2)
  }

  fun send() {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null)
    channel.queuePurge(RPC_QUEUE_NAME)

    //다수의 서버에 요청을 분산하기 위해 prefetchCount를 1로 설정한다.
    channel.basicQos(1)

    println(" [x] Awaiting RPC requests")

    val deliverCallback = DeliverCallback { _, delivery ->
      val replyProps = AMQP.BasicProperties.Builder()
        .correlationId(delivery.properties.correlationId)
        .build()

      val message = String(delivery.body, charset("UTF-8"))
      val n = message.toInt()

      println(" [.] fib($n)")
      val response = fib(n).toString()

      channel.basicPublish(
        "",
        delivery.properties.replyTo,
        replyProps,
        response.toByteArray()
      )
      channel.basicAck(delivery.envelope.deliveryTag, false)
    }

    channel.basicConsume(
      RPC_QUEUE_NAME,
      false,
      deliverCallback
    ) { _ -> }
  }
}

fun main(argv: Array<String>) {
  RpcServer().send()
}
