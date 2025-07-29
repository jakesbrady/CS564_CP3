import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom; 

/**
 * B+Tree Structure
 * Key - StudentId
 * Leaf Node should contain [ key,recordId ]
 */
class BTree {

    /**
     * Pointer to the root node.
     */
    private BTreeNode root;

    //private BTreeNode[] nodes;
    /**
     * Number of key-value pairs allowed in the tree/the minimum degree of B+Tree
     **/
    private int t;

    BTree(int t) {
        this.root = null;
        this.t = t;
    }

        //Constructor to help us with recursion
    BTree(BTreeNode root, int t) {
        this.root = root;
        this.t = t;
    }

    long search(long studentId) {
        	/**
		 * TODO: Implement this function to search in the B+Tree. Return recordID for
		 * the given StudentID. Otherwise, print out a message that the given studentId
		 * has not been found in the table and return -1.
		 */
		BTreeNode currentNode = root;
		while (!currentNode.leaf) {
			long[] keys = currentNode.keys;

			int i;
			// okay so a little weird to use the for loop like this, but we just need to
			// increment i
			for (i = 0; i <= keys.length && currentNode.children[i + 1] != null && studentId >= keys[i]; i++);
			currentNode = currentNode.children[i];
		}

		int i;
		for (i = 0; i < currentNode.n && currentNode.keys[i] != studentId; i++);

		if (currentNode.keys[i] == studentId) {
			return currentNode.values[i];
		} else {
			return -1;
		}
    }

//JSB: This is a helper to get the child of the passed in curNode, assumes that the passed in curNode is not a leaf.
	//Used by insert. Andrew to insert into seach if he wants.
private BTreeNode getChild(BTreeNode curNode, long studentID) {
	int i;
	//The following increments i just enough to get to the child of the current node that guides us closer to a potential node that may contain student id. 
	//We walk through the keys array, and compare student id against the key value, and then its cooresponsing children to find where we need to go next.
	for (i = 0; i < curNode.keys.length && curNode.children[i + 1] != null && studentID >= curNode.keys[i]; i++);
	return curNode.children[i];
}

    //JSB: IN PROGRESS
    //Inserts an entry into subtree.
    //ASSUMPTIONS:
    //root=nodepointer = this.root (the tree we are inserting into)
    //degree is d
    //need to track isNewChildEntry
    BTree insert(Student student) {
        /**
         * TODO:
         * Implement this function to insert in the B+Tree.
         * Also, insert in student.csv after inserting in B+Tree.
         */
        //initialize things
        
        
        BTree updatedTree = insert(
        return this;
    }

    //helper to recursively insert
    private BTree insert(BTreeNode nodepointer, Student student, BTreeNode newchildentry) {
        if(!nodepointer.leaf) {
	    int i; //track subtree 
            int studentKey = student.studentId;
            for(i=0; i < nodepointer.keys.length; i++) { 
                if(studentKey >= nodepointer.keys[i]) { 
		    //TO DO: set i
		}
	    }
	    insert(nodepointer,student,newchildentry);
	    if(newchildentry.next==null) {
	        return;
	    } else {
		if(nodepointer.n < 2 * t + 1) { //node has space
		  //put newchildentry on it  
		} else {
		    
		}
            }
         } else { //we found the leaf node (where we are inserting)
	    if(nodepointer.n < nodepointer.keys.length) { // number of k/v pairs < size of keys array? There is space!
		nodepointer.n++; //adding a new k/v pair
		nodepointer.keys[n-1]=student.studentId;
		//TO DO: Insert record id into values (what is record id??)
		newchildentry = null;
		return;
	    } else { //leaf is full :(
		BTreeNode L2 = splitNodes(nodepointer, d);
		newchildentry.keys[0] = L2.keys[0];
		newchildentry.next = L2;
		return;
	    }
        }
        return this;
    }

//JSB helper
//function to split a node into 2 nodes. Returns the newly created node, and updates the original to contain the
private BTreeNode splitNodes(BTreeNode nodeToSplit, int d) {
    BTreeNode L2 = new BTreeNode(d, nodeToSplit.leaf);
    // Move keys: from d+1 to 2d into L2
    for (int i = 0; i < d; i++) {
        L2.keys[i] = nodeToSplit.keys[d + 1 + i];
        nodeToSplit.keys[d + 1 + i] = 0; // clear old reference
    }
    // For leaf: move values too
    if (nodeToSplit.leaf) {
        for (int i = 0; i < d; i++) {
            L2.values[i] = nodeToSplit.values[d + 1 + i];
            nodeToSplit.values[d + 1 + i] = 0;
        }
        // Handle next pointer
        L2.next = nodeToSplit.next;
        nodeToSplit.next = L2;
    } else {
        // Move children: from d+1 to 2d+1 into L2
        for (int i = 0; i < d + 1; i++) {
            L2.children[i] = nodeToSplit.children[d + 1 + i];
            nodeToSplit.children[d + 1 + i] = null;
        }
    }
    // Update counts
    L2.n = d;
    nodeToSplit.n = d;
    // Note: nodeToSplit.keys[d] (the median) will be promoted
    // Leave it intact or extract it in the caller
    return L2;
}


    boolean delete(long studentId) {
        BTree tempTree = delete(studentId, this);

        if(tempTree != null)
        {
            this.root = tempTree.root;
        }

        return tempTree == null;
    }

    private BTree delete(long studentId, BTree tree)
    {

        long recordId = search(studentId);

        //Sanity check
        if(recordId == -1)
        {
            //not found
            return null;
        }

        //loop through each subnode
        for(BTreeNode node : tree.root.children)
        {
            //We're in the right node, but are we at the leaf yet?
            if(!node.leaf)
            {
                //No; we'll want to keep going, but once we're done, we'll want to rebuild the tree and send it back up
                BTree tempTree = delete(recordId, new BTree(node, t));

                //Check for merges
                for(BTreeNode mergedNode: tempTree.root.children)
                {
                    if(mergedNode.children.length < mergedNode.n) //There was a merge
                    {
                        BTreeNode[] updatedChildren = new BTreeNode[mergedNode.children.length - 1];
                        for(int i = 0; i < mergedNode.children.length; i ++)
                        {
                            //Check the value of each parent node against the largest of the child nodes
                            if(mergedNode.keys[i] <= mergedNode.children[i].keys[mergedNode.children[i].keys.length - 1])
                            {
                                //Update all but the node that was merged
                                updatedChildren[i] = mergedNode.children[i];
                            }
                        }
                        //Update the merged node
                        mergedNode.children = updatedChildren;
                        mergedNode.n--;

                        if(mergedNode.n < t/2)
                        {
                            //Oops we have to merge this one, too
                            mergedNode = merge(mergedNode);
                            //We don't have to worry about the children; they're already taken care of
                        }
                    }
                    
                    for(int i = 0; i < mergedNode.children.length - 1; i ++)
                    {
                        //Update pointers to the values of the smallest element in the *next* node
                        mergedNode.keys[i] = mergedNode.children[i + 1].keys[0];
                        mergedNode.values[i] = mergedNode.children[i + 1].values[0];
                    }
                }

                return tempTree;
            }
            else
            {
                //We're in a leaf node; find the element to delete
                long[] updatedKeys = new long[node.keys.length - 1];
                long[] updatedValues = new long[node.values.length - 1];
                for(int i = 0; i < node.keys.length; i++)
                {
                    if(node.values[i] == recordId) //this is the element to delete
                    {
                        //loop through and add all but the element in question to the temp arrays
                        for(int j=0; j < node.keys.length; j++)
                        {
                            if(j != i)
                            {
                                updatedKeys[j] = node.keys[j];
                                updatedValues[j] = node.values[j];
                                node.n--;

                                //Remove from Student.csv        
                                try
                                {
                                    File studentFile = new File("src/Student.csv");
                                    File tempStudentFile = new File("src/TempStudent.csv");
                                    Scanner scan = new Scanner(studentFile);
                                    FileWriter writer = new FileWriter(tempStudentFile);
                                    while(scan.hasNextLine())
                                    {
                                        String nextLine = scan.nextLine();
                                        String fileStudentId = nextLine.substring(0,nextLine.indexOf(','));
                                        
                                        //write all lines except the match to a temporary file
                                        if(Long.parseLong(fileStudentId) != studentId)
                                        {
                                            writer.write(nextLine);
                                        }
                                    }
                                    scan.close();
                                    writer.close();

                                    //Overwrite the original student file with the temporary one
                                    tempStudentFile.renameTo(studentFile);

                                }
                                catch (FileNotFoundException e)
                                {
                                    System.out.println("File not found.");
                                    e.printStackTrace();
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }

                //update the node with the "new" keys/values
                node.keys = updatedKeys;
                node.values = updatedValues;
                node.n--;

                if(node.n < t/2)
                {
                    //This node is now too empty; we need to either rebalance or merge
                    if(node.next != null && node.next.n > t/2)
                    {
                        //The next node has an element to spare

                        //Add the first element of the next node to the end of this node
                        node.keys[node.keys.length] = node.next.keys[0];
                        node.values[node.values.length] = node.next.values[0];

                        //Remove the first element of the next node
                        long[] updatedNextKeys = new long[node.next.keys.length - 1];
                        long[] updatedNextValues = new long[node.next.values.length - 1];
                        for(int i = 1; i <node.next.n; i++)
                        {
                                updatedNextKeys[i] = node.next.keys[i];
                                updatedNextValues[i] = node.next.values[i];
                        }
                        //update the next node with the "new" keys/values
                        node.next.keys = updatedNextKeys;
                        node.next.values = updatedNextValues;
                        node.next.n--;
                    }

                    else if(node.previous != null && node.previous.n > t/2)
                    {
                        //The previous node has an element to spare

                        //Add the last element of the previous node to the beginning of this node
                        long[] extendedKeys = new long[node.next.keys.length + 1];
                        long[] extendedValues = new long[node.next.values.length + 1];
                        for(int i = node.n; i >= 1; i--)
                        {
                            //Go backwards through the array, copying the value ahead one index
                            extendedKeys[i] = node.keys[i-1];
                            extendedValues[i] = node.values[i-1];
                        }
                        node.keys = extendedKeys;
                        node.values = extendedValues;
                        node.keys[0] = node.previous.keys[node.previous.keys.length - 1];
                        node.values[0] = node.previous.values[node.previous.values.length - 1];

                        //Remove the last element of the previous node
                        node.previous.keys = Arrays.copyOf(node.previous.keys, node.previous.keys.length -1);
                        node.previous.values = Arrays.copyOf(node.previous.values, node.previous.values.length -1);
                        node.previous.n--;
                    }

                    else
                    {
                        //We have to merge; move all the elements from the next node into this one
                        node = merge(node);
                    }
                }
            }
        }
        return null;
    }

    private BTreeNode merge(BTreeNode nodeToMerge)
    {
        long[] mergedKeys = new long[nodeToMerge.n + nodeToMerge.next.n];
        long[] mergedValues = new long[nodeToMerge.n + nodeToMerge.next.n];
                            
        //Add this node's elements
        for(int i = 0; i < nodeToMerge.n; i++)
        {
            mergedKeys[i] = nodeToMerge.keys[i];
            mergedValues[i] = nodeToMerge.keys[i];
        }
                            
        //Add the next node's elements
        for(int i = 0; i < nodeToMerge.next.n; i++)
        {
            //Keep going where you left off
            mergedKeys[nodeToMerge.n - 1 + i] = nodeToMerge.next.keys[i];
            mergedValues[nodeToMerge.n - 1 + i] = nodeToMerge.next.keys[i];
        }

        //Update the node
        nodeToMerge.keys = mergedKeys;
        nodeToMerge.values = mergedValues;
        nodeToMerge.n += nodeToMerge.next.n;
                            
        //Can't forget to update the pointers
        nodeToMerge.next = nodeToMerge.next.next; 
        nodeToMerge.next.previous = nodeToMerge;
        return nodeToMerge;
    }
    List<Long> print() {

        List<Long> listOfRecordID = new ArrayList<>();

		/**
		 * TODO: Implement this function to print the B+Tree. Return a list of recordIDs
		 * from left to right of leaf nodes.
		 *
		 */
		BTreeNode currentNode = root;
		while (!currentNode.leaf) {
			currentNode = currentNode.children[0];
		}
		while (currentNode.next != null) {
			for (int i = 0; i < currentNode.n; i++) {
				listOfRecordID.add(currentNode.values[i]);
			}
		}
		return listOfRecordID;
    }
}
