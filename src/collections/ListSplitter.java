package collections;

import static java.lang.Math.ceil;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static java.util.stream.IntStream.range;

import java.util.ArrayList;
import java.util.List;

/***
 * A small program to split a list into multiple sublists based on a given
 * threshold limit on the number of elements that can be present in the list.
 * 
 * My Use Case : To Split the cassandra BatchStatement into multiple BatchStatements in case if it exceeds the limit of 65535 statements within one batch.   
 * 
 * @author Siva Narayana Reddy M [siva.narayanareddy.maddula@gmail.com]. Created
 *         on 22-JAN-2016.
 *
 */
public class ListSplitter {
	
	@SuppressWarnings("serial")
	public static <E> List<List<E>> split(List<E> list, int limit) {
		final int inputSize = list.size();
		final int sublistsCount = (int) ceil( (double) inputSize / limit);
		return new ArrayList<List<E>>() {{
			range(0, sublistsCount).forEach(i -> {
				add(list.subList(i * limit,  (limit + (i * limit)) < inputSize ? (limit + (i * limit)) : inputSize));
			});
		}};
	}
	
	
	public static void main(String[] args) {
		
		// A list containing numbers from 1 to 108
		List<Integer> numbers = rangeClosed(1, 108).boxed().collect(toList());
		
		System.out.println("************************ Input List *******************");
		System.out.println(numbers);
		System.out.println("************************ Output Lists *****************");
		ListSplitter.split(numbers, 10).forEach(System.out::println);
	}
	

}



/*
 
OUTPUT : 

************************ Input List *****************
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108]

************************ Output Lists *****************
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
[11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
[21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
[31, 32, 33, 34, 35, 36, 37, 38, 39, 40]
[41, 42, 43, 44, 45, 46, 47, 48, 49, 50]
[51, 52, 53, 54, 55, 56, 57, 58, 59, 60]
[61, 62, 63, 64, 65, 66, 67, 68, 69, 70]
[71, 72, 73, 74, 75, 76, 77, 78, 79, 80]
[81, 82, 83, 84, 85, 86, 87, 88, 89, 90]
[91, 92, 93, 94, 95, 96, 97, 98, 99, 100]
[101, 102, 103, 104, 105, 106, 107, 108]

*/