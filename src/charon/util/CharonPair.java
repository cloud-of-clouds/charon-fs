package charon.util;

import java.io.Serializable;

public class CharonPair<K,V> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6005334589695918533L;
	private K k;
	private V v;
	
	public CharonPair(K k, V v) {
		this.k = k;
		this.v = v;
	}
	
	public K getK() {
		return k;
	}
	
	public V getV() {
		return v;
	}
	
	public void setK(K k) {
		this.k = k;
	}
	
	public void setV(V v) {
		this.v = v;
	}

}
