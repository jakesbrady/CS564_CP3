import java.util.ArrayList;
import java.util.List;

/**
 * B+Tree Structure Key - studentId Leaf Node should contain [ key,recordId ]
 */
class BTree {

	/**
	 * Pointer to the root node.
	 */
	private BTreeNode root;

	/**
	 * Number of key-value pairs allowed in the tree/the minimum degree of B+Tree
	 **/
	private int t;

	BTree(int t) {
		this.root = null;
		this.t = t;
	}

	/**
	 * searches for a student
	 * @param studentId
	 * @return their record ID or -1 if they could not be found
	 */
	long search(long studentId) {
		if (root == null) {
			return -1;
		}
		BTreeNode currentNode = getLeaf(studentId);

		int i = 0;
		while (i < currentNode.n - 1 && currentNode.keys[i] != studentId) {
			i++;
		}

		if (currentNode.keys[i] == studentId) {
			return currentNode.values[i];
		} else {
			return -1;
		}
	}

	/**
	 * gets the leaf that might have studentId
	 * @param studentId
	 * @return the leaf that might have studentId
	 */
	private BTreeNode getLeaf(long studentId) {
		BTreeNode currentNode = root;
		while (!currentNode.leaf) {
			currentNode = getChild(currentNode, studentId);
		}
		return currentNode;
	}

	/**
	 * Finds the next child that might have a given studentId
	 * @param curNode the node you want to find the studentId on
	 * @param studentId
	 * @return the next node to check
	 */
	private BTreeNode getChild(BTreeNode curNode, long studentId) {
		return curNode.children[getChildIndex(curNode, studentId)];
	}

	/**
	 * gets the index of the next node to check for the studentId
	 * @param curNode the node you want to find the studentId on
	 * @param studentId
	 * @return the index
	 */
	private int getChildIndex(BTreeNode curNode, long studentId) {
		int i = 0;
		// The following increments i just enough to get to the child of the current
		// node that guides us closer to a potential node that may contain student id.
		// We walk through the keys array, and compare student id against the key value,
		// and then its corresponding children to find where we need to go next.
		while (i < curNode.keys.length && curNode.children[i + 1] != null && studentId >= curNode.keys[i]) {
			i++;
		}
		return i;
	}

	/**
	 * Similar to getChildIndex but finds the location assuming you want to insert a new entry at that location
	 * @param curNode the node you want to insert into
	 * @param studentId
	 * @return the right child index to insert at (shoving the children at that index and higher to the right)
	 */
	private int getInsertIndex(BTreeNode curNode, long studentId) {
		int i = 0;
		// The following increments i just enough to get to the child of the current
		// node that guides us closer to a potential node that may contain student id.
		// We walk through the keys array, and compare student id against the key value,
		// and then its cooresponsing children to find where we need to go next.
		if (curNode.leaf) {
			while (i < curNode.n && studentId >= curNode.keys[i]) {
				i++;
			}
		} else {
			while (i < curNode.keys.length && curNode.children[i] != null && studentId >= curNode.keys[i]) {
				i++;
			}
		}
		return i;
	}

	/**
	 * gets where a curNode is in the parentNode
	 * @param parentNode the node to find curNode in
	 * @param curNode the node to find
	 * @return the index to find it at
	 */
	private int getMyIndex(BTreeNode parentNode, BTreeNode curNode) {
		int i = 0;
		while (i < parentNode.children.length - 1 && parentNode.children[i] != curNode) {
			i++;
		}
		if (parentNode.children[i] == curNode) {
			return i;
		} else {
			return -1;
		}
	}

	/**
	 * insert an entry into the tree
	 * @param student student to insert
	 * @return the tree
	 */
	BTree insert(Student student) {
		// initialize things
		if (root == null) {
			root = new BTreeNode(t, true);
		}
		BTreeNode newChild = insert(root, student);
		if (newChild != null) { // if the function returns it just did a split on roots so we need to grow the
								// tree heigh.
			BTreeNode oldRoot = root;
			root = new BTreeNode(t, false);
			root.children[0] = oldRoot;
			root.children[1] = newChild;
			root.keys[0] = getSmallestKey(newChild);
			newChild.previous = oldRoot;
			oldRoot.next = newChild;
			root.n = 1;
		}
		return this;
	}

	/**
	 * recursive call 
	 * @param nodePointer the current node we are checking
	 * @param student student to insert
	 * @return null or a a freshly split node if one was just made
	 */
	private BTreeNode insert(BTreeNode nodePointer, Student student) {
		if (!nodePointer.leaf) {

			int childIndex = getChildIndex(nodePointer, student.studentId);
			BTreeNode newChildEntry = insert(nodePointer.children[childIndex], student);
			if (newChildEntry == null) {
				return null;
			} else {
				if (nodePointer.children[nodePointer.keys.length] == null) { // node has space
					int placeToInsert = getInsertIndex(nodePointer, getSmallestKey(newChildEntry));
					addEnrty(nodePointer, newChildEntry, placeToInsert);
					return null;
				} else {
					if (childIndex >= t) {
						BTreeNode L2 = splitNodes(nodePointer, true);
						int placeToInsert = getInsertIndex(L2, getSmallestKey(newChildEntry));
						addEnrty(L2, newChildEntry, placeToInsert);
						return L2;
					} else {
						BTreeNode L2 = splitNodes(nodePointer, false);
						int placeToInsert = getInsertIndex(nodePointer, getSmallestKey(newChildEntry));
						addEnrty(L2, newChildEntry, placeToInsert);
						return L2;
					}

				}

			}
		} else { // we found the leaf node (where we are inserting)
			if (nodePointer.n < nodePointer.keys.length) { // number of k/v pairs < size of keys array? There is space!
				int placeToInsert = getInsertIndex(nodePointer, student.studentId);
				addEnrty(nodePointer, student, placeToInsert);

				return null;
			} else { // leaf is full :(
				BTreeNode L2 = splitNodes(nodePointer);
				int placeToInsert = getInsertIndex(L2, student.studentId);
				if (placeToInsert == 0) {
					placeToInsert = getInsertIndex(nodePointer, student.studentId);
					addEnrty(nodePointer, student, placeToInsert);
				} else {
					addEnrty(L2, student, placeToInsert);
				}
				return L2;
			}
		}
	}

	/**
	 * adds a student to a leaf node
	 * @param nodePointer the node to insert it in
	 * @param student student to insert
	 * @param addIndex the index to insert at as per keys array
	 */
	private void addEnrty(BTreeNode nodePointer, Student student, int addIndex) {
		addEnrty(nodePointer, student.studentId, student.recordId, addIndex);
	}

	/**
	 * adds a student to a leaf node
	 * @param nodePointer the node to insert into
	 * @param studentId their Id
	 * @param recordId their recordId
	 * @param addIndex the index to add it at as per keys array
	 */
	private void addEnrty(BTreeNode nodePointer, long studentId, long recordId, int addIndex) {
		for (int i = nodePointer.n; i > addIndex; i--) {
			nodePointer.keys[i] = nodePointer.keys[i - 1];
			nodePointer.values[i] = nodePointer.values[i - 1];
		}
		nodePointer.n++; // adding a new k/v pair
		nodePointer.keys[addIndex] = studentId;
		nodePointer.values[addIndex] = recordId;
	}

	/**
	 * adds a node to another node
	 * @param nodePointer the node you are adding it to
	 * @param nodeToAdd the node to add to it
	 * @param addIndex the index to put it at
	 */
	private void addEnrty(BTreeNode nodePointer, BTreeNode nodeToAdd, int addIndex) {

		for (int i = (nodePointer.keys.length - 1); i >= addIndex; i--) {
			if (nodePointer.children[i] != null) {
				nodePointer.keys[i] = nodePointer.keys[i - 1];
				nodePointer.children[i + 1] = nodePointer.children[i];
			}
		}
		nodePointer.n++;
		nodePointer.children[addIndex] = nodeToAdd;
		if (addIndex != 0) {
			nodePointer.keys[addIndex - 1] = getSmallestKey(nodeToAdd);
		} else {
			nodePointer.keys[0] = getSmallestKey(nodePointer.children[1]);
		}
		if (addIndex + 1 < nodePointer.children.length && nodePointer.children[addIndex + 1] != null) {
			nodeToAdd.next = nodePointer.children[addIndex + 1];
			nodePointer.children[addIndex + 1].previous = nodeToAdd;
		}
		if (addIndex - 1 > 0) {
			nodePointer.previous = nodePointer.children[addIndex - 1];
			nodePointer.children[addIndex - 1].next = nodeToAdd;
		}

	}

	/**
	 * gets the smallest key in a leaf node (useful for re-keying)
	 * @param nodePointer the node to get the smallest key of
	 * @return the smallest key of any entry
	 */
	private long getSmallestKey(BTreeNode nodePointer) {
		BTreeNode curNode = nodePointer;
		while (!curNode.leaf) {
			curNode = curNode.children[0];
		}
		return curNode.keys[0];
	}

	/**
	 * splits the leaf node into multiple
	 * @param nodeToSplit the node to split
	 * @return the new node generated from the split
	 */
	private BTreeNode splitNodes(BTreeNode nodeToSplit) {
		BTreeNode L2 = new BTreeNode(t, nodeToSplit.leaf);
		// Move keys: from d+1 to 2d into L2
		for (int i = 0; i < t; i++) {
			L2.keys[i] = nodeToSplit.keys[t + i];
			nodeToSplit.keys[t + i] = 0; // clear old reference
		}
		// For leaf: move values too
		for (int i = 0; i < t; i++) {
			L2.values[i] = nodeToSplit.values[t + i];
			nodeToSplit.values[t + i] = 0;
		}
		// Handle next pointer
		L2.next = nodeToSplit.next;
		nodeToSplit.next = L2;
		L2.previous = nodeToSplit;

		// Update counts
		L2.n = t;
		nodeToSplit.n = t;
		// Note: nodeToSplit.keys[d] (the median) will be promoted
		// Leave it intact or extract it in the caller
		return L2;
	}

	/**
	 * splits non-leaf node into multiple
	 * @param nodeToSplit the node to split
	 * @param moreLeft needed to bias the split more to the left or more to the right based on what side it should be inserted
	 * @return the new node generated from the split
	 */
	private BTreeNode splitNodes(BTreeNode nodeToSplit, boolean moreLeft) {
		BTreeNode L2 = new BTreeNode(t, nodeToSplit.leaf);
		// Move keys: from d+1 to 2d into L2

		// For leaf: move values too

		if (moreLeft) {
			for (int i = 1; i < t; i++) {
				L2.keys[i - 1] = nodeToSplit.keys[t + i];
				nodeToSplit.keys[t + i] = 0; // clear old reference
			}
			nodeToSplit.keys[t] = 0;
			// Move children: from d+1 to 2d+1 into L2
			for (int i = 1; i < t + 1; i++) {
				L2.children[i - 1] = nodeToSplit.children[t + i];
				nodeToSplit.children[t + i] = null;
			}
			nodeToSplit.keys[t] = 0;
			// Update counts
			L2.n = t - 1;
			nodeToSplit.n = t;
		} else {
			for (int i = 0; i < t; i++) {
				L2.keys[i] = nodeToSplit.keys[t + i];
				nodeToSplit.keys[t + i] = 0; // clear old reference
			}
			// Move children: from d+1 to 2d+1 into L2
			for (int i = 0; i < t + 1; i++) {
				L2.children[i] = nodeToSplit.children[t + i];
				nodeToSplit.children[t + i] = null;
			}
			nodeToSplit.keys[t - 1] = 0;
			// Update counts
			L2.n = t;
			nodeToSplit.n = t - 1;
		}

		// Note: nodeToSplit.keys[d] (the median) will be promoted
		// Leave it intact or extract it in the caller
		return L2;
	}

	/**
	 * deletes an entry from the tree
	 * @param studentId
	 * @return if the deletion worked
	 */
	boolean delete(long studentId) {
		if (search(studentId) == -1) {
			return false;
		} else {
			if (root.leaf) {
				removeEnrty(root, studentId);
			} else {
				int childIndex = getChildIndex(root, studentId);
				BTreeNode oldChildEntry = delete(root, root.children[childIndex], studentId);
				if (oldChildEntry != null) {
					removeEnrty(root, oldChildEntry);
					if (root.children[1] == null) {
						root = root.children[0];
					}
				}
			}
			return true;
		}
	}

	/**
	 * recursive function to delete the entry
	 * @param parentPointer the parent of the node we are focusing on
	 * @param nodePointer the node we are focusing on
	 * @param studentId the studentId to delete
	 * @return null, or an entry that needs to be removed from the parent
	 */
	private BTreeNode delete(BTreeNode parentPointer, BTreeNode nodePointer, long studentId) {
		if (!nodePointer.leaf) {

			int childIndex = getChildIndex(nodePointer, studentId);
			BTreeNode oldChildEntry = delete(nodePointer, nodePointer.children[childIndex], studentId);
			if (oldChildEntry == null) {
				return null;
			} else {
				removeEnrty(nodePointer, oldChildEntry);
				int myIndex = getMyIndex(parentPointer, nodePointer);
				if (nodePointer.children[t] != null) {
					return null;
				} else if (myIndex - 1 > 0 && parentPointer.children[myIndex - 1].children[t + 1] != null) {
					parentPointer.keys[myIndex - 1] = redistribute(parentPointer.children[myIndex - 1], nodePointer);
					return null;
				} else if (myIndex + 1 < parentPointer.children.length && parentPointer.children[myIndex + 1] != null
						&& parentPointer.children[myIndex + 1].children[t + 1] != null) {
					parentPointer.keys[myIndex] = redistribute(nodePointer, parentPointer.children[myIndex + 1]);
					return null;
				} else if (myIndex - 1 >= 0) {
					merge(parentPointer.children[myIndex - 1], nodePointer);
					return nodePointer;
				} else {
					merge(nodePointer, parentPointer.children[myIndex + 1]);
					return parentPointer.children[myIndex + 1];
				}
			}
		} else {
			removeEnrty(nodePointer, studentId);
			int myIndex = getMyIndex(parentPointer, nodePointer);
			if (nodePointer.n >= t) {
				return null;
			} else if (myIndex - 1 > 0 && parentPointer.children[myIndex - 1].n > t) {
				parentPointer.keys[myIndex - 1] = redistribute(parentPointer.children[myIndex - 1], nodePointer);
				return null;
			} else if (myIndex + 1 < parentPointer.children.length && parentPointer.children[myIndex + 1] != null
					&& parentPointer.children[myIndex + 1].n > t) {
				parentPointer.keys[myIndex] = redistribute(nodePointer, parentPointer.children[myIndex + 1]);
				return null;
			} else if (myIndex - 1 >= 0) {
				merge(parentPointer.children[myIndex - 1], nodePointer);
				return nodePointer;
			} else {
				merge(nodePointer, parentPointer.children[myIndex + 1]);
				return parentPointer.children[myIndex + 1];
			}
		}

	}

	/**
	 * removeEnrty removes a node from another node
	 * @param nodePointer node you are removing from
	 * @param entryToRemove the node you are removing
	 */
	private void removeEnrty(BTreeNode nodePointer, BTreeNode entryToRemove) {
		boolean found = false;
		for (int i = 0; i < nodePointer.children.length; i++) {
			if (found) {
				if (i == (nodePointer.children.length - 1)) {
					nodePointer.children[i] = null;
					nodePointer.keys[i - 1] = 0;
				} else {
					nodePointer.children[i] = nodePointer.children[i + 1];
					if (i > 0)
						nodePointer.keys[i - 1] = nodePointer.keys[i];
				}
			} else {
				if (nodePointer.children[i] == entryToRemove) {
					found = true;
					if (i == (nodePointer.children.length - 1)) {
						nodePointer.children[i] = null;
						nodePointer.keys[i - 1] = 0;
					} else {
						nodePointer.children[i] = nodePointer.children[i + 1];
						if (i > 0)
							nodePointer.keys[i - 1] = nodePointer.keys[i];
					}
				}
			}
		}
		nodePointer.n--;
	}

	/**
	 * removes a student from a leaf node
	 * @param nodePointer the pointer to remove the student from
	 * @param studentId the id of the student to remove
	 */
	private void removeEnrty(BTreeNode nodePointer, long studentId) {
		boolean found = false;
		for (int i = 0; i < nodePointer.keys.length; i++) {
			if (found) {
				if (i == (nodePointer.keys.length - 1)) {
					nodePointer.keys[i] = 0;
					nodePointer.values[i] = 0;
				} else {
					nodePointer.keys[i] = nodePointer.keys[i + 1];
					nodePointer.values[i] = nodePointer.values[i + 1];
				}
			} else {
				if (nodePointer.keys[i] == studentId) {
					found = true;
					if (i == (nodePointer.keys.length - 1)) {
						nodePointer.keys[i] = 0;
						nodePointer.values[i] = 0;
					} else {
						nodePointer.keys[i] = nodePointer.keys[i + 1];
						nodePointer.values[i] = nodePointer.values[i + 1];
					}
				}
			}
		}
		nodePointer.n--;

	}

	/**
	 * evenly splits nodes or values between the nodes (remainder left biased)
	 * @param leftNode the left node to redistribute
	 * @param rightNode the right node to redistribute
	 * @return the key to use between these two nodes
	 */
	private long redistribute(BTreeNode leftNode, BTreeNode rightNode) {
		if (leftNode.leaf) {
			int total = (leftNode.n + rightNode.n);
			int remainder = total % 2;
			int leftTarget = (total / 2) + remainder;
			while (leftNode.n < leftTarget) {
				leftNode.keys[leftNode.n] = rightNode.keys[0];
				leftNode.values[leftNode.n] = rightNode.values[0];
				removeEnrty(rightNode, rightNode.keys[0]);
				leftNode.n++;
			}
			while (leftNode.n > leftTarget) {
				addEnrty(rightNode, leftNode.keys[leftNode.n - 1], leftNode.values[leftNode.n - 1], 0);
				leftNode.keys[leftNode.n - 1] = 0;
				leftNode.values[leftNode.n - 1] = 0;
				leftNode.n--;
			}
			return rightNode.keys[0];
		} else {
			int countLeft = 0;
			int countRight = 0;
			for (int i = 0; i < leftNode.children.length; i++) {
				if (leftNode.children[i] != null)
					countLeft = i + 1;
				if (rightNode.children[i] != null)
					countRight = i + 1;
			}
			int total = countLeft + countRight;
			int remainder = total % 2;
			int leftTarget = (total / 2) + remainder;
			for (int i = countLeft - 1; i > leftTarget; i--) {

				addEnrty(rightNode, leftNode.children[i], 0);
				leftNode.children[i] = null;
				leftNode.keys[i - 1] = 0;
				leftNode.n--;
			}
			for (int i = countLeft; i < leftTarget; i++) {
				leftNode.children[i] = rightNode.children[0];
				leftNode.keys[i - 1] = rightNode.children[0].keys[0]; // we can't use the key from right. have to look
																		// it up
				removeEnrty(rightNode, rightNode.children[0]);
				leftNode.n++;
			}
			return getSmallestKey(rightNode);
		}
	}

	/**
	 * merge the right node into the left node
	 * @param leftNode will get all the stuff
	 * @param rightNode will be completely barren
	 */
	private void merge(BTreeNode leftNode, BTreeNode rightNode) {
		if (leftNode.leaf) {
			int leftTarget = (leftNode.n + rightNode.n);
			while (leftNode.n < leftTarget) {
				leftNode.keys[leftNode.n] = rightNode.keys[0];
				leftNode.values[leftNode.n] = rightNode.values[0];
				leftNode.n++;
				removeEnrty(rightNode, rightNode.keys[0]);
			}
		} else {
			int countLeft = 0;
			int countRight = 0;
			for (int i = 0; i < leftNode.children.length; i++) {
				if (leftNode.children[i] != null)
					countLeft = i + 1;
				if (rightNode.children[i] != null)
					countRight = i + 1;
			}
			int leftTarget = countLeft + countRight;
			for (int i = countLeft; i < leftTarget; i++) {
				leftNode.children[i] = rightNode.children[0];
				leftNode.keys[i - 1] = rightNode.children[0].keys[0]; // we can't use the key from right. have to look
																		// it up
				removeEnrty(rightNode, rightNode.children[0]);
				leftNode.n++;
			}
		}
	}

	/**
	 * puts all the recordIds in a list for easy printing
	 * @return a list of recordIds
	 */
	List<Long> print() {

		List<Long> listOfRecordID = new ArrayList<>();

		BTreeNode currentNode = root;
		while (!currentNode.leaf) {
			currentNode = currentNode.children[0];
		}
		for (int i = 0; i < currentNode.n; i++) {
			listOfRecordID.add(currentNode.values[i]);
		}
		while (currentNode.next != null) {
			currentNode= currentNode.next;
			for (int i = 0; i < currentNode.n; i++) {
				listOfRecordID.add(currentNode.values[i]);
			}
		}
		return listOfRecordID;
	}
}
