package com.bfxy.springboot.conusmer;

import java.util.Map;

import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

@Component
public class RabbitReceiver {

	
	@RabbitListener(bindings = @QueueBinding(
			value = @Queue(value = "queue-1", 
			durable="true"),
			exchange = @Exchange(value = "exchange-1", 
			durable="true", 
			type= "topic", 
			ignoreDeclarationExceptions = "true"),
			key = "springboot.*"
			)
	)
	@RabbitHandler
	public void onMessage(Message message, Channel channel) throws Exception {
		System.err.println("--------------------------------------");
		System.err.println(Thread.currentThread().getName() + "  消费端Payload: " + message.getPayload());

		System.err.println(Thread.currentThread().getName() + "  发送ack");
		Long deliveryTag = (Long)message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
		//手工ACK
		channel.basicAck(deliveryTag, false);

	}


	/**
	 * 
	 * 	spring.rabbitmq.listener.order.queue.name=queue-2
		spring.rabbitmq.listener.order.queue.durable=true
		spring.rabbitmq.listener.order.exchange.name=exchange-1
		spring.rabbitmq.listener.order.exchange.durable=true
		spring.rabbitmq.listener.order.exchange.type=topic
		spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions=true
		spring.rabbitmq.listener.order.key=springboot.*
	 	payload 指引入实体对象
	 * @param order
	 * @param channel
	 * @param headers
	 * @throws Exception
	 */
	@RabbitListener(bindings = @QueueBinding(
			value = @Queue(value = "${spring.rabbitmq.listener.order.queue.name}", 
			durable="${spring.rabbitmq.listener.order.queue.durable}"),
			exchange = @Exchange(value = "${spring.rabbitmq.listener.order.exchange.name}", 
			durable="${spring.rabbitmq.listener.order.exchange.durable}", 
			type= "${spring.rabbitmq.listener.order.exchange.type}", 
			ignoreDeclarationExceptions = "${spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions}"),
			key = "${spring.rabbitmq.listener.order.key}"
			)
	)
	@RabbitHandler
	public void onOrderMessage(@Payload com.bfxy.springboot.entity.Order order, 
			Channel channel, 
			@Headers Map<String, Object> headers) throws Exception {
		System.err.println("--------------------------------------");
		System.err.println("消费端order: " + order.getId());
		Long deliveryTag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
		// 手工ACK
		// ack 确认消息
		// 第二个参数 multiple ，用于批量确认消息，为了减少网络流量，手动确认可以被批处。
		// 1. 当 multiple 为 true 时，则可以一次性确认 deliveryTag 小于等于传入值的所有消息
		// 2. 当 multiple 为 false 时，则只确认当前 deliveryTag 对应的消息
		channel.basicAck(deliveryTag, false);
	}


	/**
	 * 订阅测试消费者
	 * @param message
	 * @param channel
	 * @throws Exception
	 */
	@RabbitListener(bindings = @QueueBinding(
			value = @Queue(value = "queue-umap",
					durable="true"),
			exchange = @Exchange(value = "umap_qc-business_task_asr_result",
					durable="true",
					type= "direct",
					ignoreDeclarationExceptions = "true"),
			key = "xxx-asr_result-umap_qc"
	)
	)
	@RabbitHandler
	public void onMessage1(Message message, Channel channel) throws Exception {
		System.err.println("--------------------------------------");
		System.err.println(Thread.currentThread().getName() + "  消费端Payload: " + message.getPayload());

		System.err.println(Thread.currentThread().getName() + "  发送ack");
		Long deliveryTag = (Long)message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
		System.err.println(Thread.currentThread().getName() + " DELIVERY_TAG为: " + deliveryTag);
		//手工ACK
		channel.basicAck(deliveryTag, false);
	}
	
	
}
