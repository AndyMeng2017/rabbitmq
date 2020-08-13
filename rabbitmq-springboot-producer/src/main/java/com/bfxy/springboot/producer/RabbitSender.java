package com.bfxy.springboot.producer;

import java.util.Map;
import java.util.UUID;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.bfxy.springboot.entity.Order;

@Component
public class RabbitSender {

	//自动注入RabbitTemplate模板类
	@Autowired
	private RabbitTemplate rabbitTemplate;  
	
	//回调函数: confirm确认
	final ConfirmCallback confirmCallback = new RabbitTemplate.ConfirmCallback() {
		@Override
		public void confirm(CorrelationData correlationData, boolean ack, String cause) {
			System.err.println("correlationData: " + correlationData);
			System.err.println("ack: " + ack);
			if(!ack){
				System.err.println("异常处理....");
			}
		}
	};
	
	//回调函数: return返回
	final ReturnCallback returnCallback = new RabbitTemplate.ReturnCallback() {
		@Override
		public void returnedMessage(org.springframework.amqp.core.Message message, int replyCode, String replyText,
				String exchange, String routingKey) {
			System.err.println("return exchange: " + exchange + ", routingKey: " 
				+ routingKey + ", replyCode: " + replyCode + ", replyText: " + replyText);
		}
	};
	
	//发送消息方法调用: 构建Message消息
	public void send(Object message, Map<String, Object> properties) throws Exception {
		MessageHeaders mhs = new MessageHeaders(properties);
		// 这里设置的header不会生效，因为在 rabbitTemplate.convertAndSend 方法内部会进行 message 转化，因为这里的
		// org.springframework.messaging.Message != org.springframework.amqp.core.Message 坑啊 见方法 send3()
		Message msg = MessageBuilder.createMessage(message, mhs);

		rabbitTemplate.setConfirmCallback(confirmCallback);
		rabbitTemplate.setReturnCallback(returnCallback);
		//id + 时间戳 全局唯一 
		CorrelationData correlationData = new CorrelationData("1234567890");
//		rabbitTemplate.convertAndSend("exchange-1", "springboot.abc", msg, correlationData);
		// 在页面直接创建queue，会采用默认的Auto消息确认模式
		//rabbitTemplate.convertAndSend("exchange-1", "springb.abc.mhn", msg, correlationData);
		rabbitTemplate.convertAndSend("exchange-2", "springboot.a", msg, correlationData);

		//发送延迟队列会失败
		//rabbitTemplate.convertAndSend("test_delayed_exchange", "test_delayed", msg, correlationData);
	}
	
	//发送消息方法调用: 构建自定义对象消息
	public void sendOrder(Order order) throws Exception {
		rabbitTemplate.setConfirmCallback(confirmCallback);
		rabbitTemplate.setReturnCallback(returnCallback);
		//id + 时间戳 全局唯一 
		CorrelationData correlationData = new CorrelationData("0987654321");
		rabbitTemplate.convertAndSend("exchange-2", "springboot.def", order, correlationData);
	}

	/**
	 * 携带头部信息的发送到队列中
	 * @param message
	 * @throws Exception
	 */
	public void send3(Object message) throws Exception {
		String str = message.toString();
		org.springframework.amqp.core.Message message1 = org.springframework.amqp.core.MessageBuilder.withBody(str.getBytes())
				.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN)
				.setContentEncoding("utf-8")
				.setMessageId(UUID.randomUUID()+"")
				.setHeader("x-delay", 1000 * 30)
				.build();
		//id + 时间戳 全局唯一
		CorrelationData correlationData = new CorrelationData("1234567890");
		rabbitTemplate.convertAndSend("test_delayed_exchange", "test_delayed", message1, correlationData);
	}
	
}
