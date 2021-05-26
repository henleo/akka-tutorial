package de.hpi.ddm.actors;

import de.hpi.ddm.structures.BytesMessage;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
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
		this.frameSize = 1000;
		this.nextMessageToBeSentId = 0;
		this.messagesToBeSent = new HashMap<Long, BytesMessage<?>>();
		this.messagesBeingReceived = new HashMap<ActorRef, byte[]>();
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
	public static class RequestToSendMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		private long messageToBeSentId;
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
	
	enum Ack implements Serializable {
	INSTANCE;
	}

	@Data @NoArgsConstructor
	static class StreamInitialized implements Serializable {
		private static final long serialVersionUID = 1L;
	}

	@Data @NoArgsConstructor
	static class StreamCompleted implements Serializable {
		private static final long serialVersionUID = 1L;
	}

	@Data
	static class StreamFailure implements Serializable {
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
	HashMap<Long, BytesMessage<?>> messagesToBeSent;
	HashMap<ActorRef, byte[]> messagesBeingReceived;
	
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
		sender().tell(Ack.INSTANCE, self());
		// create new datastructure to collect elements in
		this.messagesBeingReceived.put(sender(), new byte[0]);
	}

	private void handle(byte[] element) {
		log().info("Received element: {}", element);
		sender().tell(Ack.INSTANCE, self());
		// append new element to elements received so far
		byte[] messageReceivedSoFar = this.messagesBeingReceived.get(sender());
		byte[] updatedMessage = new byte[messageReceivedSoFar.length + element.length];
		System.arraycopy(messageReceivedSoFar, 0, updatedMessage, 0, messageReceivedSoFar.length);
		System.arraycopy(element, 0, updatedMessage, messageReceivedSoFar.length, element.length);
		this.messagesBeingReceived.put(sender(), updatedMessage);
	}

	private void handle(StreamCompleted completed) {
		log().info("Stream completed");
		// deserialize message
		byte[] serializedMessage = this.messagesBeingReceived.get(sender());
		BytesMessage<?> deserializedMessage = (BytesMessage<?>) KryoPoolSingleton.get().fromBytes(serializedMessage);
		this.messagesBeingReceived.remove(sender());
		// send to receiver
		deserializedMessage.getReceiver().tell(deserializedMessage.getBytes(), deserializedMessage.getSender());
	}

	private void handle(StreamFailure failed) {
		log().error(failed.getCause(), "Stream failed!");
	}

	private void handle(RequestToSendMessage request) {
		sender().tell(new AckToSendMessage(request.messageToBeSentId), this.self());
	}

	private void handle(TimeoutMessage timeout) {
		if (this.messagesToBeSent.containsKey(timeout.messageToBeSentId)) {
			// send new RequestToSendMessage to other proxy
			ActorRef receiver = this.messagesToBeSent.get(timeout.messageToBeSentId).getReceiver();
			ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
			receiverProxy.tell(new RequestToSendMessage(timeout.messageToBeSentId), this.self());

			// schedule new timeout to self
			TimeoutMessage timeoutMessage = new TimeoutMessage(timeout.messageToBeSentId);
			this.getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10), this.self(), timeoutMessage, this.getContext().dispatcher(), null);
		}
	}

	private void handle(AckToSendMessage ack) {
		if(this.messagesToBeSent.containsKey(ack.messageToBeSentId)) {

			// get acknowledged message 
			log().info("handling ack message, not serialized yet");
			BytesMessage<?> byteMessage = this.messagesToBeSent.get(ack.messageToBeSentId);
			log().info("right before serializing");
			byte[] serializedMessage = KryoPoolSingleton.get().toBytesWithClass(byteMessage);
			log().info("handling ack message, bytemessage serialized");

			// split byte array of the serialized message in frames of fixed size and put them into an arraylist 
			ArrayList<byte[]> serializedMessageList = new ArrayList<byte[]>();
			int numberOfFramesNeeded = (serializedMessage.length + this.frameSize - 1)/this.frameSize;
			for(int i = 0; i < numberOfFramesNeeded; i++){
				if(i < numberOfFramesNeeded - 1) {
					serializedMessageList.add(Arrays.copyOfRange(serializedMessage, i * this.frameSize, ((i+1) * this.frameSize) - 1));
				}
				else {
					// last frame might be shorter than framSize
					serializedMessageList.add(Arrays.copyOfRange(serializedMessage, i * this.frameSize, serializedMessage.length - 1));
				}
			}

			// stream serialized message to other proxy (sender of this acknowledgement)

			Source<byte[], NotUsed> messageStream = Source.from(serializedMessageList);

			ActorSystem system = this.getContext().getSystem();

			Sink<byte[], NotUsed> sink =
				Sink.<byte[]>actorRefWithBackpressure(
					sender(),
					new StreamInitialized(),
					Ack.INSTANCE,
					new StreamCompleted(),
					ex -> new StreamFailure(ex));

			messageStream.runWith(sink, system);

			// delete from messagesToBeSent
			this.messagesToBeSent.remove(ack.messageToBeSentId);
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
		
		BytesMessage<?> byteMessage = new BytesMessage<>(message, sender, receiver);
		
		// store serialized message in messagesToBeSent, send request to other proxy 
		this.messagesToBeSent.put(this.nextMessageToBeSentId, byteMessage);
		receiverProxy.tell(new RequestToSendMessage(this.nextMessageToBeSentId), this.self());

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
