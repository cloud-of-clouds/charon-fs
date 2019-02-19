package charon.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MapWVersionControl <K,V extends CharonPair<Integer, byte[]>> {

	private Map<K, V> map;
	private LinkedList<K> list;

	private int memoryUsed;

	public MapWVersionControl() {
		map = new HashMap<K,V>();
		list = new LinkedList<K>();
		memoryUsed = 0;
	}

	public CharonPair<K, V> discardOldest(){
		synchronized (map) {
			synchronized (list) {
				K k;
				V v;
				if(!list.isEmpty()){
					k = list.removeLast();
					v = map.remove(k);
					if(v!=null && v.getV() != null)
						memoryUsed -= v.getV().length;
					return new CharonPair<K, V>(k, v);
				}
				return null;
			}
		}

	}

	public V discard(K k){
		synchronized (map) {
			synchronized (list) {
				list.remove(k);
				V v = map.remove(k);
				if(v!=null && v.getV() != null) {
					memoryUsed -= v.getV().length;
				}
				return v;
			}
		}
	}

	public boolean contains(K k){
		synchronized (map) {
			return map.containsKey(k);
		}
	}

	public void put(K k,V v){
		synchronized (map) {
			V aux = map.put(k,v);
			if(aux!=null){
				memoryUsed = (memoryUsed - aux.getV().length) + v.getV().length;
				swap(k);
			}else{
				memoryUsed+=v.getV().length;
				synchronized (list) {
					list.addFirst(k);
				}
			}
		}
	}

	public V get(K k){
		synchronized (map) {
			V res = map.get(k);
			if(res!=null)
				swap(k);
			return res;
		}
	}

	private void swap(K k){
		synchronized (list) {
			list.remove(k);
			list.addFirst(k);
		}
	}

	public int getMemoryUsed (){
		return memoryUsed;
	}


}
