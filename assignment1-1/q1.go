package cos418_hw1_1

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"io/ioutil"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

	// Build the regex
	reg := regexp.MustCompile("[^0-9a-zA-Z]+")

	// Read the content from the file
	fileBytes, err := ioutil.ReadFile(path)
	checkError(err)

	// Convert the content bytes array to string
	fileString := string(fileBytes)
	lowerFileString := strings.ToLower(fileString)

	// Split in words
	words := strings.Fields(lowerFileString)

	// Organize them in a map
	wordsMap := make(map[string]int)
	for _, word := range words {
		// Build safe words based only-characters rules
		safeWord := reg.ReplaceAllString(word, "")

		// Only the words that fit the 'charThreshold' are added to the map
		if len(safeWord) >= charThreshold {
			wordsMap[safeWord]++
		}
	}

	// Build the WordCount array using the map values
	countArray := make([]WordCount, 0)

	for key, value := range wordsMap {
		countArray = append(countArray, WordCount{Word: key, Count: value})
	}

	// Sort the list
	sortWordCounts(countArray)

	// Return only the indicated number of records
	return countArray[0:numWords]
}

// WordCount ...A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
