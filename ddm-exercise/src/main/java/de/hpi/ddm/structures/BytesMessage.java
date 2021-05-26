package de.hpi.ddm.structures;

import java.io.Serializable;

import akka.actor.ActorRef;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor
public class BytesMessage<T> implements Serializable {
	private static final long serialVersionUID = 4057807743872319842L;
	public T bytes;
	public ActorRef sender;
	public ActorRef receiver;
}