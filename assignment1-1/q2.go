package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`

	// Accumulate a sum in this worker
	mySum := 0
	for value := range nums {
		mySum += value
	}

	// Return the worker sum
	out <- mySum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// Initialize a reader associated to the fileNames
	file, err := os.Open(fileName)
	checkError(err)
	fileReader := bufio.NewReader(file)

	// Pass the reader to the readInts function and get the array of numbers
	numbers, err := readInts(fileReader)
	checkError(err)

	// Split the read numbers in 'num' parts to distribute them in the workers
	portionSize := len(numbers) / num

	// Instantiate channels for in and out
	numsChannel := make(chan int, portionSize)
	outChannel := make(chan int, num)

	// Initialize all workers
	for i := 0; i < num; i++ {
		go sumWorker(numsChannel, outChannel)
	}

	// Send the numbers to the nums channel
	for _, value := range numbers {
		numsChannel <- value
	}

	// Close the nums channel
	close(numsChannel)

	// Accumulate all the workers results
	finalResult := 0
	for i := 0; i < num; i++ {
		singleResult := <-outChannel
		finalResult += singleResult
	}

	return finalResult
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
