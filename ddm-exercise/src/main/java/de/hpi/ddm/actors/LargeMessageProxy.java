package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
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
		this.messageReceivedSoFar = new ArrayList<Byte>();
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

	static class StreamInitialized {}

	static class StreamCompleted {}

	static class StreamFailure {
	private final Throwable cause;

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

	ArrayList<Byte> messageReceivedSoFar;
	
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
					this.messageReceivedSoFar.clear();
					})
				.match(
					Byte.class,
					element -> {
					log().info("Received element: {}", element);
					sender().tell(Ack.INSTANCE, self());
					//TODO: put elements back together
					this.messageReceivedSoFar.add(element);
					})
				.match(
					StreamCompleted.class,
					completed -> {
					log().info("Stream completed");
					//TODO: deserialize message
					Byte[] completeMessage = new Byte[this.messageReceivedSoFar.size()];
					completeMessage = this.messageReceivedSoFar.toArray(completeMessage);
					KryoPool kryo = KryoPoolSingleton.get();
					BytesMessage<?> deserializedMessage = kryo.fromBytes(completeMessage);
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
		
		KryoPool kryo = KryoPoolSingleton.get();

		BytesMessage<?> byteMessage = new BytesMessage<>(message, sender, receiver);
		byte[] serializedMessage = kryo.toBytesWithClass(byteMessage);
		

		Source<Byte, NotUsed> messageStream = Source.from(serializedMessage);

		Sink<String, NotUsed> sink =
			Sink.<Byte>actorRefWithBackpressure(
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
