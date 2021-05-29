package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Deque;
import java.util.Queue;
import java.util.HashSet;
import java.util.Set;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
		
		this.paramsAreSet = false;
		this.pwLength = 0;
		this.numberOfHints = 0;
		this.characterSet = new HashSet<Character>();

		this.reachedEOF = false;
		this.newBatchIncoming = false;

		this.pwsCurrentlyWorkingOn = new HashMap<Long, PwsWorkingOnEntry>();
		this.packagesToBeAssigned = new LinkedList<WorkPackage>();
		this.pwPackagesWaitingForHints = new HashMap<Long, WorkPackage>();
		this.freeWorkers = new LinkedList<ActorRef>();
		this.assignedPackages = new HashMap<ActorRef, WorkPackage>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @AllArgsConstructor
	public static class ResultMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public final String result;
		public final long pwId;
		public final int packageInsidePwId;
		public final boolean isPassword;

		public ResultMessage() {
			this.result = new String();
			this.pwId = (long) 0;
			this.packageInsidePwId = 0;
			this. isPassword = false;
		}
	}

	@Data @AllArgsConstructor
	private class PwsWorkingOnEntry {
		public final String name;
		public final List<Character> solvedHints;
	}

	@Data @AllArgsConstructor
	public static class WorkPackage implements Serializable {
		private static final long serialVersionUID = 1L;
		public final String work;
		public final long pwId;
		public final int packageInsidePwId;
		public final boolean isPassword;

		public WorkPackage() {
			this.work = new String();
			this.pwId = (long) 0;
			this.packageInsidePwId = 0;
			this.isPassword = false;
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;

	private long startTime;

	private boolean paramsAreSet;
	private int pwLength;
	private int numberOfHints;
	private Set<Character> characterSet;

	private boolean reachedEOF;
	private boolean newBatchIncoming;

	private final HashMap<Long, PwsWorkingOnEntry> pwsCurrentlyWorkingOn;
	private final Deque<WorkPackage> packagesToBeAssigned;
	private final HashMap<Long, WorkPackage> pwPackagesWaitingForHints;
	private final Queue<ActorRef> freeWorkers;
	private final HashMap<ActorRef, WorkPackage> assignedPackages;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(ResultMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.requestNewBatch();
	}
	
	protected void handle(BatchMessage message) {
		
		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		// TODO: Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		this.newBatchIncoming = false;

		if (message.getLines().isEmpty()) {
			this.reachedEOF = true;
			return;
		}
		// set dataset params
		if(!this.paramsAreSet) {
			String[] firstLine = message.getLines().get(0);
			this.numberOfHints = firstLine.length - 5;
			this.pwLength = Integer.valueOf(firstLine[3]);
			char[] characterSetArray = firstLine[2].toCharArray();
			this.characterSet = new HashSet<Character>();
			for(int i = 0; i < characterSetArray.length; i++) {
				this.characterSet.add(characterSetArray[i]);
			}
			this.paramsAreSet = true;
		}

		// TODO: Process the lines with the help of the worker actors
		for (String[] line : message.getLines()) {
			//this.log().error("Need help processing: {}", Arrays.toString(line));
			//store as workpackages in assignment queue and in waiting queue and as pwsworkingon entries
			long passwordId = Long.valueOf(line[0]);

			// make entry in list of pws we are working on
			PwsWorkingOnEntry entry = new PwsWorkingOnEntry(line[1], new ArrayList<Character>());
			this.pwsCurrentlyWorkingOn.put(passwordId, entry);

			// create work package for pw, put in waiting for hints register
			String hashedPassword = line[4];
			WorkPackage pwPackage = new WorkPackage(hashedPassword, passwordId, 0, true);
			this.pwPackagesWaitingForHints.put(passwordId, pwPackage);

			// create work packages for hints, put in waiting for assignment queue
			for(int i = 5; i < this.numberOfHints + 5; i++) {
				String hashedHint = line[i];
				WorkPackage hintPackage = new WorkPackage(hashedHint, passwordId, i-4, false);
				this.packagesToBeAssigned.add(hintPackage);
			}
		}
		// check if any free workers, if yes assign packages to them, if out of packages, new read request
		while(!this.freeWorkers.isEmpty()) {
			ActorRef worker = this.freeWorkers.remove();
			this.assignPackageFromQueue(worker);
		}
		//this.log().error("made it through all package assigns of first batch");
	}

	private void requestNewBatch() {
		if(!this.newBatchIncoming) {
			this.reader.tell(new Reader.ReadMessage(), this.self());
			this.newBatchIncoming = true;
		}
	}

	private void assignPackageFromQueue(ActorRef worker) {
		if(!(this.reachedEOF && this.packagesToBeAssigned.isEmpty())) {
			if(this.packagesToBeAssigned.isEmpty()) {
				this.requestNewBatch();
				this.freeWorkers.add(worker);
			} else {
				//build package message
				WorkPackage workPackage = this.packagesToBeAssigned.remove();
				long passwordId = workPackage.getPwId();
				List<Character> hintList = this.pwsCurrentlyWorkingOn.get(passwordId).getSolvedHints();
				Set<Character> hints = new HashSet<Character>(hintList);
				Worker.WorkPackageMessage message = new Worker.WorkPackageMessage(workPackage, this.characterSet, this.pwLength, hints);
				// assign package
				//worker.tell(message, this.self());
				this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(message, worker), this.self());
				this.assignedPackages.put(worker, workPackage);

			}
		} else {
			if(!this.freeWorkers.contains(worker)) {
				this.freeWorkers.add(worker);
			}
		}
	}
	
	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		//this.log().info("Registered {}", this.sender());
		
		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());
		
		// TODO: Assign some work to registering workers. Note that the processing of the global task might have already started.
		this.assignPackageFromQueue(this.sender());
	}

	private void handle(ResultMessage message) {
		// handle result

		// clear package from assignedpackages
		this.assignedPackages.remove(this.sender());

		long passwordId = message.getPwId();
		PwsWorkingOnEntry entry = this.pwsCurrentlyWorkingOn.get(passwordId);

		if(message.isPassword) {
			//check if last pw (eof, empty package queue, empty pw queue, empty pws working on, empty assigned register) if yes, terminate
			//else assign new package
			String resultString = "ID: " + String.valueOf(message.getPwId()) + " Name: " + entry.name + " Password: " + message.getResult();
			this.collector.tell(new Collector.CollectMessage(resultString), this.self());

			this.pwsCurrentlyWorkingOn.remove(passwordId);

			if(this.reachedEOF && this.packagesToBeAssigned.isEmpty() && this.pwPackagesWaitingForHints.isEmpty() && this.pwsCurrentlyWorkingOn.isEmpty() /*&& this.assignedPackages.isEmpty()*/) {
				this.terminate();
			} else {
				this.assignPackageFromQueue(this.sender());
			}
		} else {
			//TODO add hint to pwworkingon entry, checkif last hint, then queue pw package and remove it from waitingforhints queue
			entry.solvedHints.add(this.getCharacterFromHint(message.getResult()));
			if(entry.solvedHints.size() == this.numberOfHints) {
				WorkPackage pwWorkPackage = this.pwPackagesWaitingForHints.get(passwordId);
				this.pwPackagesWaitingForHints.remove(passwordId);
				this.packagesToBeAssigned.addFirst(pwWorkPackage);
			}
			this.assignPackageFromQueue(this.sender());
		}
	}

	private char getCharacterFromHint(String hint) {
		for(char c : this.characterSet) {
			if(!hint.contains(String.valueOf(c))){
				return c;
			}
		}
		this.log().error("HintNotMissingAnyCharacter");
		return ' ';
	}
	
	public static class HintNotMissingAnyCharacter extends Exception {}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		//this.log().info("Unregistered {}", message.getActor());
		//if worker was working on package, put that package in the front of waiting to be assigned queue 
		//and remove relevant entry from assignedPackages
		if(this.freeWorkers.contains(message.getActor())) {
			this.freeWorkers.remove(message.getActor());
		}
		if(this.assignedPackages.containsKey(message.getActor())) {
			WorkPackage workPackage = this.assignedPackages.remove(message.getActor());
			this.packagesToBeAssigned.addFirst(workPackage);
		}
	}
}
