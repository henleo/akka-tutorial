package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);

		this.foundResult = false;
		this.result = new String();
		this.hashedResult = new String();
		this.iterations = (long) 0;
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}

	@Data @AllArgsConstructor
	public static class WorkPackageMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public final Master.WorkPackage workPackage;
		public final Set<Character> characterSet;
		public final int pwLength;
		public final Set<Character> hints;

		public WorkPackageMessage() {
			this.workPackage = new Master.WorkPackage(new String(), (long) 0, 0, false);
			this.characterSet = new HashSet<Character>();
			this.pwLength = 0;
			this.hints = new HashSet<Character>();
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;

	private boolean foundResult;
	private String result;
	private String hashedResult;

	private long iterations;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
				.match(WorkPackageMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
			
			this.registrationTime = System.currentTimeMillis();
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
	}
	
	private String hash(String characters) {

		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));

			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}

			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	// a: character set, size: size of character set, n: length of permutation (for truncating result)
	private void heapPermutation(char[] a, int size, int n/*, List<String> l*/) {
		// If size is 1, store the obtained permutation
		if(this.foundResult){
			return;
		}

		if (size == 1) {
			//l.add(new String(a));
			String permutation = new String(a);
			permutation = permutation.substring(0, n);
			this.iterations++;
			if(this.hash(permutation).equals(this.hashedResult)) {
				this.foundResult = true;
				this.result = permutation;
			}
		} else {
			for (int i = 0; i < size; i++) {
				heapPermutation(a, size - 1, n/*, l*/);

				// If size is odd, swap first and last element
				if (size % 2 == 1) {
					char temp = a[0];
					a[0] = a[size - 1];
					a[size - 1] = temp;
				}

				// If size is even, swap i-th and last element
				else {
					char temp = a[i];
					a[i] = a[size - 1];
					a[size - 1] = temp;
				}
			}
		}
	}

	// set: character set, prefix: empty string, n: length of character set, k: length of the permutations
	private void getAllKLengthRec(char[] set, String prefix, int n, int k)
	{
		if(this.foundResult) {
			return;
		}
		// Base case: k is 0,
		// print prefix
		if (k == 0)
		{
			this.iterations++;
			//System.out.println(prefix);
			if(this.hash(prefix).equals(hashedResult)) {
				this.foundResult = true;
				this.result = prefix;
			}
			return;
		}
	
		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{
	
			// Next character of input added
			String newPrefix = prefix + set[i];
			
			// k is decreased, because
			// we have added a new character
			getAllKLengthRec(set, newPrefix, n, k - 1);
		}
	}

	private void handle(WorkPackageMessage message) {
		//this.log().error("start handling WorkPackageMessage: " + "pwId: " + String.valueOf(message.getWorkPackage().getPwId()) + " packageId: " + String.valueOf(message.getWorkPackage().getPackageInsidePwId()));
		this.foundResult = false;
		this.result = new String();
		this.hashedResult = message.getWorkPackage().getWork();
		Set<Character> characterSet = new HashSet<Character>();
		boolean isPassword = message.getWorkPackage().isPassword;
		//prune hint characters from character set if this is a password
		if(isPassword) {
			for(char c : message.getCharacterSet()) {
				if(!message.getHints().contains(c)) {
					characterSet.add(c);
				}
			}
		}
		else {
			characterSet = message.getCharacterSet();
		}
		List<Character> characterSetList = new ArrayList<>(characterSet);
		char[] characterSetArray = characterSetList.stream().map(Object::toString).collect(Collectors.joining()).toCharArray();
		this.iterations = (long) 0;
		if(isPassword) {
			this.getAllKLengthRec(characterSetArray, new String(), characterSetArray.length, message.getPwLength());
		} else {
			this.heapPermutation(characterSetArray, message.getCharacterSet().size(), message.getCharacterSet().size() - 1);
		}
		//this.log().error("handling WorkPackageMessage: result found: " + this.result + " iterations: " + String.valueOf(this.iterations) + " pwId: " + String.valueOf(message.getWorkPackage().getPwId()) + " packageId: " + String.valueOf(message.getWorkPackage().getPackageInsidePwId()));
		Master.ResultMessage resultMessage = new Master.ResultMessage(this.result, message.getWorkPackage().getPwId(), message.getWorkPackage().getPackageInsidePwId(), isPassword);
		//this.sender().tell(resultMessage, this.self());
		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(resultMessage, this.sender()), this.self());
	}
}