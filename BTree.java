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

    private BTreeNode[] nodes;
    /**
     * Number of key-value pairs allowed in the tree/the minimum degree of B+Tree
     **/
    private int t;

    BTree(int t) {
        this.root = null;
        this.nodes = null;
        this.t = t;
    }

        //Constructor to help us with recursion
    BTree(BTreeNode root, BTreeNode[] nodes, int t) {
        this.root = root;
        this.nodes = nodes;
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
			for (i = 0; i < keys.length && currentNode.children[i + 1] != null && studentId > keys[i]; i++)
				;
			currentNode = currentNode.children[i];
		}

		int i;
		for (i = 0; i < currentNode.n && currentNode.keys[i] != studentId; i++)
			;

		if (currentNode.keys[i] == studentId) {
			return currentNode.values[i];
		} else {
			return -1;
		}
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
    private BTree insert(BTreeNode nodepointer, Student student, boolean newchildentry) {
        
        if(!root.leaf) {
            int studentKey = student.studentId;
            for(int i=0; i < root.keys.size(); i++) {
                if(studentKey >= root.keys[i]) {
                    //this is the subtree
                    
                }
            }
    
        } else { //is not leaf node
        
        }
        return this;
        
    }


    boolean delete(long studentId) {
        /**
         * TODO:
         * Implement this function to delete in the B+Tree.
         * Also, delete in student.csv after deleting in B+Tree, if it exists.
         * Return true if the student is deleted successfully otherwise, return false.
         */

        BTree tempTree = delete(studentId, this);

        if(tempTree != null)
        {
            this.root = tempTree.root;
            this.nodes = tempTree.nodes;
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

        //TODO: check the root node first?

        //loop through each subnode
        for(BTreeNode node : tree.nodes)
        {
            //Check the last/biggest value in this node; if it's too small, keep going
            if (recordId <= node.values[node.values.length - 1])
            {
                //We're in the right node, but are we at the leaf yet?
                if(!node.leaf)
                {
                    //No; we'll want to keep going, but once we're done, we'll want to rebuild the tree and send it back up
                    BTree tempTree = delete(recordId, new BTree(node, node.children, t));

                    //Check for merges
                    for(BTreeNode mergedNode: tempTree.nodes)
                    {
                        if(mergedNode.children.length < mergedNode.n) //There was a merge
                        {
                            //TODO: update the values of mergedNode with the valus of each of the children
                            mergedNode.n--;
                        }
                        
                        //TODO: check for rebalanced nodes
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

                    if(node.n < t/2)
                    {
                        //This node is now too empty; we need to either rebalance or merge
                        if(node.next != null && node.next.n >= t/2)
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

                        else if(node.previous != null && node.previous.n >= t/2)
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
                            long[] mergedKeys = new long[node.n + node.next.n];
                            long[] mergedValues = new long[node.n + node.next.n];
                            
                            //Add this node's elements
                            for(int i = 0; i < node.n; i++)
                            {
                                mergedKeys[i] = node.keys[i];
                                mergedValues[i] = node.keys[i];
                            }
                            
                            //Add the next node's elements
                            for(int i = 0; i < node.next.n; i++)
                            {
                                //Keep going where you left off
                                mergedKeys[node.n - 1 + i] = node.next.keys[i];
                                mergedValues[node.n - 1 + i] = node.next.keys[i];
                            }

                            //Update the node
                            node.keys = mergedKeys;
                            node.values = mergedValues;
                            node.n += node.next.n;
                            
                            //can't forget to update the pointers
                            node.next = node.next.next; 
                            node.next.previous = node;
                        }
                    }
                }
            }
        }
        return null;
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
