package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * this class make sure all field is not modifiable for external class
 */
public class OperatorDescriptor {

	private final Integer operatorID;
	private final String name;

	private final Set<OperatorDescriptor> parents = new HashSet<>();
	private final Set<OperatorDescriptor> children = new HashSet<>();

	private final OperatorPayload payload;
	private boolean stateful = false;

	public OperatorDescriptor(Integer operatorID, String name, int parallelism) {
		this.operatorID = operatorID;
		this.name = name;
		this.payload = new OperatorPayload(parallelism);
	}

	public Integer getOperatorID() {
		return operatorID;
	}

	public String getName() {
		return name;
	}

	public Set<OperatorDescriptor> getParents() {
		return Collections.unmodifiableSet(parents);
	}

	public Set<OperatorDescriptor> getChildren() {
		return Collections.unmodifiableSet(children);
	}

	public int getParallelism() {
		return payload.parallelism;
	}

	public Function getUdf() {
		Object functionObject = payload.applicationLogic.attributeMap.get(ApplicationLogic.UDF);
		return (Function) Preconditions.checkNotNull(functionObject);
	}

//	public List<List<Integer>> getKeyStateAllocation() {
//		return Collections.unmodifiableList(payload.keyStateAllocation);
//	}

	public Map<Integer, List<Integer>> getKeyStateAllocation() {
		return payload.keyStateAllocation;
	}

	public Map<Integer, Map<Integer, List<Integer>>> getKeyMapping() {
		return Collections.unmodifiableMap(payload.keyMapping);
	}

	public void setParallelism(int parallelism) {
		payload.parallelism = parallelism;
	}

	public void setUdf(Function udf) {
		payload.applicationLogic.udf = udf;
	}

	public final void setControlAttribute(String name, Object obj) throws Exception {
		payload.applicationLogic.updateField(name, obj);
	}

	public Map<String, Object> getControlAttributeMap() {
		return Collections.unmodifiableMap(payload.applicationLogic.attributeMap);
	}

	@Internal
//	void setKeyStateAllocation(List<List<Integer>> keyStateAllocation) {
	void setKeyStateAllocation(Map<Integer, List<Integer>> keyStateAllocation) {
//		List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
//			keyStateAllocation.stream()
//				.map(Collections::unmodifiableList)
//				.collect(Collectors.toList())
//		);
//		payload.keyStateAllocation.addAll(unmodifiableKeys);
		addAll(keyStateAllocation);
		for (OperatorDescriptor parent : parents) {
			parent.payload.keyMapping.put(operatorID, keyStateAllocation);
		}
		// stateless operator should not be allocated  key set
		stateful = !payload.keyStateAllocation.isEmpty();
	}

	private void addAll(Map<Integer, List<Integer>> keyStateAllocation) {
//		payload.keyStateAllocation.clear();
		Map<Integer, List<Integer>> unmodifiable = new HashMap<>();
		for (int taskId : keyStateAllocation.keySet()) {
			unmodifiable.put(taskId, Collections.unmodifiableList(keyStateAllocation.get(taskId)));
		}
		payload.keyStateAllocation = Collections.unmodifiableMap(unmodifiable);
	}

	@Internal
	void setKeyMapping(Map<Integer, Map<Integer, List<Integer>>> keyMapping) {
		Map<Integer, Map<Integer, List<Integer>>> unmodifiable = convertKeyMappingToUnmodifiable(keyMapping);
		payload.keyMapping.putAll(unmodifiable);
		for (OperatorDescriptor child : children) {
			if (child.stateful) {
				// todo two inputs?
//				child.payload.keyStateAllocation.addAll(keyMapping.get(child.operatorID));
				child.addAll(keyMapping.get(child.operatorID));
			}
		}
	}

	@Internal
	ApplicationLogic getApplicationLogic(){
		return payload.applicationLogic;
	}

	private Map<Integer, Map<Integer, List<Integer>>> convertKeyMappingToUnmodifiable(Map<Integer, Map<Integer, List<Integer>>> keyMappings) {
		Map<Integer, Map<Integer, List<Integer>>> unmodifiable = new HashMap<>();
		for (Integer inOpID : keyMappings.keySet()) {
//			List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
////				keyStateAllocation.get(inOpID)
////					.stream()
////					.map(Collections::unmodifiableList)
////					.collect(Collectors.toList())
//			);
			Map<Integer, List<Integer>> keyStateAllocation = convertKeyStateToUnmodifiable(keyMappings.get(inOpID));

			Map<Integer, List<Integer>> unmodifiableKeys = Collections.unmodifiableMap(keyStateAllocation);
			unmodifiable.put(inOpID, unmodifiableKeys);
		}
		return unmodifiable;
	}

	private Map<Integer, List<Integer>> convertKeyStateToUnmodifiable(Map<Integer, List<Integer>> keyStateAllocation) {
		Map<Integer, List<Integer>> newMap = new HashMap<>();
		for (Integer taskId : keyStateAllocation.keySet()) {
			newMap.put(taskId, Collections.unmodifiableList(newMap.get(taskId)));
		}
		return newMap;
	}

	/**
	 * we assume that the key operator only have one upstream opeartor
	 *
	 * @param keyStateAllocation
	 */
	@PublicEvolving
	public void setKeySet(Map<Integer, List<Integer>> keyStateAllocation) {
		if (!stateful) {
			System.out.println("not support now");
			return;
		}
		try {
//			List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
//				keyStateAllocation.stream()
//					.map(Collections::unmodifiableList)
//					.collect(Collectors.toList())
//			);
			addAll(keyStateAllocation);
			// sync with parent's key mapping
			for(OperatorDescriptor parent: parents) {
				parent.payload.keyMapping.put(operatorID, keyStateAllocation);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Only use to update key mapping for case that target operator is stateless.
	 * If the target is stateful, the key mapping should be changed by
	 * setKeyStateAllocation(List<List<Integer>> keyStateAllocation);
	 *
	 * @param targetOperatorID
	 * @param keyMapping
	 */
	@PublicEvolving
	public void setKeyMappingTo(int targetOperatorID, Map<Integer, List<Integer>> keyMapping) {
		try {
			OperatorDescriptor child = checkOperatorIDExistInSet(targetOperatorID, children);

//			List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
//				keyMapping.stream()
//					.map(Collections::unmodifiableList)
//					.collect(Collectors.toList())
//			);
			payload.keyMapping.put(targetOperatorID, keyMapping);
			if (child.stateful) {
				child.addAll(keyMapping);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static OperatorDescriptor checkOperatorIDExistInSet(int opID, Set<OperatorDescriptor> set) throws Exception {
		for (OperatorDescriptor descriptor : set) {
			if (opID == descriptor.getOperatorID()) {
				return descriptor;
			}
		}
		throw new Exception("do not have this id in set");
	}


	/**
	 * @param childEdges       the list of pair of parent id and child id to represent the relationship between operator
	 * @param allOperatorsById
	 */
	void addChildren(List<Tuple2<Integer, Integer>> childEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		// f0 is parent operator id, f1 is child operator id
		for (Tuple2<Integer, Integer> edge : childEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.f0) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.f1);
			// I think I am your father
			children.add(descriptor);
			descriptor.parents.add(this);
		}
	}

	/**
	 * @param parentEdges      the list of pair of parent id and child id to represent the relationship between operator
	 * @param allOperatorsById
	 */
	void addParent(List<Tuple2<Integer, Integer>> parentEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		// f0 is parent operator id, f1 is child operator id
		for (Tuple2<Integer, Integer> edge : parentEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.f1) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.f0);
			// I think I am your father
			parents.add(descriptor);
			descriptor.children.add(this);
		}
	}

	@Override
	public String toString() {
		return "OperatorDescriptor{name='" + name + "'', parallelism=" + payload.parallelism +
			", parents:" + parents.size() + ", children:" + children.size() + '}';
	}

	private static class OperatorPayload {
		int parallelism;
		final ApplicationLogic applicationLogic;
		/* for stateful one input stream task, the key state allocation item is always one */
//		final List<List<Integer>> keyStateAllocation;
//		final Map<Integer, List<List<Integer>>> keyMapping;

		public Map<Integer, List<Integer>> keyStateAllocation;
		public Map<Integer, Map<Integer, List<Integer>>> keyMapping;

		OperatorPayload(int parallelism) {
			this.parallelism = parallelism;
//			keyStateAllocation = new ArrayList<>(parallelism);
			keyStateAllocation = new HashMap<>();
			keyMapping = new HashMap<>();
			applicationLogic = new ApplicationLogic();
		}
	}

	void setAttributeField(Object object, List<Field> fieldList) throws IllegalAccessException {
		payload.applicationLogic.operator = object;
		for(Field field: fieldList) {
			ControlAttribute attribute = field.getAnnotation(ControlAttribute.class);
			boolean accessible = field.isAccessible();
			// temporary set true
			field.setAccessible(true);
			payload.applicationLogic.fields.put(attribute.name(), field);
			payload.applicationLogic.attributeMap.put(attribute.name(), field.get(object));
			field.setAccessible(accessible);
		}
	}

	public static class ApplicationLogic{

		public static final String UDF = "UDF";
		public static final String OPERATOR_TYPE = "OPERATOR_TYPE";

		private final Map<String, Object> attributeMap = new HashMap<>();
		private final Map<String, Field> fields = new HashMap<>();
		private Function udf;

		@VisibleForTesting
		private Object operator;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ApplicationLogic that = (ApplicationLogic) o;
			return attributeMap.equals(that.attributeMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(attributeMap);
		}

		public ApplicationLogic copyTo(ApplicationLogic that){
			that.attributeMap.clear();
			that.attributeMap.putAll(attributeMap);
			return that;
		}

		public Map<String, Object> getControlAttributeMap() {
			return Collections.unmodifiableMap(attributeMap);
		}

		public Map<String, Field> getControlAttributeFieldMap() {
			return Collections.unmodifiableMap(fields);
		}

		private void updateField(String name, Object obj) throws Exception {
			Field field = fields.get(name);
			boolean access = field.isAccessible();
			try {
				field.setAccessible(true);
				field.set(operator, obj);
				attributeMap.put(name, obj);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new Exception("update field fail", e);
			}finally {
				field.setAccessible(access);
			}
		}
	}

}
