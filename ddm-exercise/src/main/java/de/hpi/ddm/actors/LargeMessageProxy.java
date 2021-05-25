package de.hpi.ddm.actors;

import java.io.Serializable;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
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
		this.messageReceivedSoFar = new byte[0];
		this.frameSize = 1000;
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
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	enum Ack {
	INSTANCE;
	}

	@Data @NoArgsConstructor
	static class StreamInitialized {
		private static final long serialVersionUID = 1L;
	}

	@Data @NoArgsConstructor
	static class StreamCompleted {
		private static final long serialVersionUID = 1L;
	}

	@Data
	static class StreamFailure {
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

	byte[] messageReceivedSoFar;
	int frameSize;
	
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
				.match(BytesMessage.class, this::handle)
				.match(
					StreamInitialized.class,
					init -> {
					log().info("Stream initialized");
					sender().tell(Ack.INSTANCE, self());
					//TODO: create new datastructure to collect elements in
					this.messageReceivedSoFar = new byte[0];
					})
				.match(
					byte[].class,
					element -> {
					log().info("Received element: {}", element);
					sender().tell(Ack.INSTANCE, self());
					//TODO: append new element to elements received so far
					byte[] updatedMessage = new byte[this.messageReceivedSoFar.length + element.length];
					System.arraycopy(this.messageReceivedSoFar, 0, updatedMessage, 0, this.messageReceivedSoFar.length);
					System.arraycopy(element, 0, updatedMessage, this.messageReceivedSoFar.length, element.length);
					this.messageReceivedSoFar = updatedMessage;
					})
				.match(
					StreamCompleted.class,
					completed -> {
					log().info("Stream completed");
					//TODO: deserialize message
					BytesMessage<?> deserializedMessage = (BytesMessage<?>) KryoPoolSingleton.get().fromBytes(this.messageReceivedSoFar);
					//TODO: send to receiver
					deserializedMessage.getReceiver().tell(deserializedMessage.getBytes(), deserializedMessage.getSender());
					})
				.match(
					StreamFailure.class,
					failed -> {
					log().error(failed.getCause(), "Stream failed!");
					})
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(StreamInitialized init) {
		log().info("Stream initialized");
		sender().tell(Ack.INSTANCE, self());
		//TODO: create new datastructure to collect elements in
		this.messageReceivedSoFar = new byte[0];
	}

	private void handle(byte[] element) {
		log().info("Received element: {}", element);
		sender().tell(Ack.INSTANCE, self());
		//TODO: append new element to elements received so far
		byte[] updatedMessage = new byte[this.messageReceivedSoFar.length + element.length];
		System.arraycopy(this.messageReceivedSoFar, 0, updatedMessage, 0, this.messageReceivedSoFar.length);
		System.arraycopy(element, 0, updatedMessage, this.messageReceivedSoFar.length, element.length);
		this.messageReceivedSoFar = updatedMessage;
	}

	private void handle(StreamCompleted completed) {
		log().info("Stream completed");
		//TODO: deserialize message
		BytesMessage<?> deserializedMessage = (BytesMessage<?>) KryoPoolSingleton.get().fromBytes(this.messageReceivedSoFar);
		//TODO: send to receiver
		deserializedMessage.getReceiver().tell(deserializedMessage.getBytes(), deserializedMessage.getSender());
	}

	private void handle(StreamFailure failed) {
		log().error(failed.getCause(), "Stream failed!");
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
		byte[] serializedMessage = KryoPoolSingleton.get().toBytesWithClass(byteMessage);
		
		//TODO: split byte arrau in frames, put frames in list - remember to adjust the reassembling

		ArrayList<byte[]> serializedMessageList = new ArrayList<byte[]>();
		for(int i = 0; i < (serializedMessage.length + this.frameSize - 1)/this.frameSize; i++){
			serializedMessageList.add(Arrays.copyOfRange(serializedMessage, i * this.frameSize, ((i+1) * this.frameSize) -1));
		}

		//TODO: stream serialized message to other proxy

		Source<byte[], NotUsed> messageStream = Source.from(serializedMessageList);

		ActorSystem system = this.getContext().getSystem();

		Sink<byte[], NotUsed> sink =
			Sink.<byte[]>actorRefWithBackpressure(
				receiver,
				new StreamInitialized(),
				Ack.INSTANCE,
				new StreamCompleted(),
				ex -> new StreamFailure(ex));

		messageStream.runWith(sink, system);

		//receiverProxy.tell(new BytesMessage<>(message, sender, receiver), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
