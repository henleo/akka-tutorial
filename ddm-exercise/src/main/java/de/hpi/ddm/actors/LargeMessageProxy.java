package de.hpi.ddm.actors;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import de.hpi.ddm.singletons.KryoPoolSingleton;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.NotUsed;
import akka.actor.ActorSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class, () -> new LargeMessageProxy());
	}

	public LargeMessageProxy(){
		this.frameSize = 2000000;
		this.nextMessageToBeSentId = (long) 0;
		this.messagesToBeSent = new HashMap<Long, MessageToBeSent<?>>();
		this.messagesBeingReceived = new HashMap<ActorRef, byte[]>();
		this.messagesAnticipated = new HashMap<Long, RequestToSendMessage>();
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class MessageToBeSent<T> implements Serializable {
		private static final long serialVersionUID = 1L;
		public T bytes;
		public ActorRef sender;
		public ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		private Object message;
		private long messageToBeSentId;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class RequestToSendMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		private long messageToBeSentId;
		public ActorRef sender;
		public ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class AckToSendMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		private long messageToBeSentId;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class TimeoutMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		private long messageToBeSentId;
	}

	public enum Ack implements Serializable {
		INSTANCE;
	}

	@Data @NoArgsConstructor
	public static class StreamInitialized implements Serializable {
		private static final long serialVersionUID = 1L;
	}

	@Data @NoArgsConstructor
	public static class StreamCompleted implements Serializable {
		private static final long serialVersionUID = 1L;
	}

	@Data
	public static class StreamFailure implements Serializable {
		private static final long serialVersionUID = 1L;
		private final Throwable cause;

		public StreamFailure() {
			this.cause = null;
		}

		public StreamFailure(Throwable cause) {
			this.cause = cause;
		}

		public Throwable getCause() {
			return cause;
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	int frameSize;
	long nextMessageToBeSentId;
	HashMap<Long, MessageToBeSent<?>> messagesToBeSent;
	HashMap<ActorRef, byte[]> messagesBeingReceived;
	HashMap<Long, RequestToSendMessage> messagesAnticipated;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				//.match(BytesMessage.class, this::handle)
				.match(StreamInitialized.class, this::handle)
				.match(byte[].class, this::handle)
				.match(StreamCompleted.class, this::handle)
				.match(StreamFailure.class, this::handle)
				.match(RequestToSendMessage.class, this::handle)
				.match(AckToSendMessage.class, this::handle)
				.match(TimeoutMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(StreamInitialized init) {
		log().info("Stream initialized");
		this.sender().tell(Ack.INSTANCE, self());
		// create new datastructure to collect elements in
		this.messagesBeingReceived.put(this.sender(), new byte[0]);
		log().info("done handling Stream initialized");
	}

	private void handle(byte[] element) {
		//log().info("Received element: {}", element);
		this.sender().tell(Ack.INSTANCE, self());
		// append new element to elements received so far
		byte[] messageReceivedSoFar = this.messagesBeingReceived.get(this.sender());
		byte[] updatedMessage = new byte[messageReceivedSoFar.length + element.length];
		System.arraycopy(messageReceivedSoFar, 0, updatedMessage, 0, messageReceivedSoFar.length);
		System.arraycopy(element, 0, updatedMessage, messageReceivedSoFar.length, element.length);
		this.messagesBeingReceived.put(this.sender(), updatedMessage);
	}

	private void handle(StreamCompleted completed) {
		log().info("Stream completed");
		// deserialize message
		byte[] serializedMessage = this.messagesBeingReceived.get(this.sender());
		log().info("right before deserialization");
		BytesMessage deserializedMessage = (BytesMessage)KryoPoolSingleton.get().fromBytes(serializedMessage);
		log().info("right after deserialization");
		this.messagesBeingReceived.remove(this.sender());
		// send to receiver
		long messageToBeSentId = deserializedMessage.getMessageToBeSentId();
		RequestToSendMessage request = this.messagesAnticipated.get(messageToBeSentId);
		ActorRef sender = request.getSender();
		ActorRef receiver = request.getReceiver();
		receiver.tell(deserializedMessage.getMessage(), sender);
		log().info("done handling Stream completed");
	}

	private void handle(StreamFailure failed) {
		log().error(failed.getCause(), "Stream failed!");
	}

	private void handle(RequestToSendMessage request) {
		this.messagesAnticipated.put(request.getMessageToBeSentId(), request);
		this.sender().tell(new AckToSendMessage(request.messageToBeSentId), this.self());
	}

	private void handle(TimeoutMessage timeout) {
		if (this.messagesToBeSent.containsKey(timeout.messageToBeSentId)) {
			// send new RequestToSendMessage to other proxy
			log().info("request to send message timed out");
			MessageToBeSent<?> message = this.messagesToBeSent.get(timeout.messageToBeSentId);
			ActorRef receiver = message.getReceiver();
			ActorRef sender = message.getSender();
			ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
			receiverProxy.tell(new RequestToSendMessage(timeout.messageToBeSentId, sender, receiver), this.self());

			// schedule new timeout to self
			TimeoutMessage timeoutMessage = new TimeoutMessage(timeout.messageToBeSentId);
			this.getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10), this.self(), timeoutMessage, this.getContext().dispatcher(), null);
		}
	}

	private void handle(AckToSendMessage ack) {
		if(this.messagesToBeSent.containsKey(ack.messageToBeSentId)) {

			// get acknowledged message 
			log().info("handling ack message, not serialized yet");
			MessageToBeSent<?> message = this.messagesToBeSent.get(ack.messageToBeSentId);
			BytesMessage bytesMessage = new BytesMessage(message.getBytes(), ack.messageToBeSentId);
			log().info("right before serializing");
			byte[] serializedMessage = KryoPoolSingleton.get().toBytesWithClass(bytesMessage);
			log().info("handling ack message, bytemessage serialized");
			BytesMessage deserializedMessage = (BytesMessage)KryoPoolSingleton.get().fromBytes(serializedMessage);
			//log().info((String)deserializedMessage.getMessage());

			// split byte array of the serialized message in frames of fixed size and put them into an arraylist 
			ArrayList<byte[]> serializedMessageList = new ArrayList<byte[]>();
			int numberOfFramesNeeded = (serializedMessage.length + this.frameSize - 1)/this.frameSize;
			for(int i = 0; i < numberOfFramesNeeded; i++){
				if(i < numberOfFramesNeeded - 1) {
					serializedMessageList.add(Arrays.copyOfRange(serializedMessage, i * this.frameSize, (i+1) * this.frameSize));
				}
				else {
					// last frame might be shorter than framSize
					serializedMessageList.add(Arrays.copyOfRange(serializedMessage, i * this.frameSize, serializedMessage.length));
				}
			}

			// stream serialized message to other proxy (sender of this acknowledgement)

			Source<byte[], NotUsed> messageStream = Source.from(serializedMessageList);

			ActorSystem system = this.getContext().getSystem();

			Sink<byte[], NotUsed> sink =
				Sink.<byte[]>actorRefWithBackpressure(
					this.sender(),
					new StreamInitialized(),
					Ack.INSTANCE,
					new StreamCompleted(),
					ex -> new StreamFailure(ex));

			messageStream.runWith(sink, system);

			// delete from messagesToBeSent
			this.messagesToBeSent.remove(ack.messageToBeSentId);
			log().info("done handling ack message");
		}
	}

	private void handle(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.
		
		MessageToBeSent<?> messageToBeSent = new MessageToBeSent(message, sender, receiver);
		
		// store serialized message in messagesToBeSent, send request to other proxy 
		this.messagesToBeSent.put(this.nextMessageToBeSentId, messageToBeSent);
		receiverProxy.tell(new RequestToSendMessage(this.nextMessageToBeSentId, sender, receiver), this.self());

		// schedule cancellable timeout to self
		TimeoutMessage timeoutMessage = new TimeoutMessage(this.nextMessageToBeSentId);
		this.getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10), this.self(), timeoutMessage, this.getContext().dispatcher(), null);

		// increment nextMessageToBeSentId so that the next message will have a new id
		this.nextMessageToBeSentId++;


		//receiverProxy.tell(new BytesMessage<>(message, sender, receiver), this.self());
	}
/*
	private void handle(BytesMessage<?> message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
*/
}
