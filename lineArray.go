package main

import (
	"bufio"
	"io"
	"unicode/utf8"

	"github.com/zyedidia/micro/cmd/micro/highlight"
)

func runeToByteIndex(n int, txt []byte) int {
	if n == 0 {
		return 0
	}

	count := 0
	i := 0
	for len(txt) > 0 {
		_, size := utf8.DecodeRune(txt)

		txt = txt[size:]
		count += size
		i++

		if i == n {
			break
		}
	}
	return count
}

// A Line contains the data in bytes as well as a highlight state, match
// and a flag for whether the highlighting needs to be updated
type Line struct {
	data []byte // change byte to pair? not a good idea

	state       highlight.State
	match       highlight.LineMatch
	rehighlight bool
}

// A LineArray simply stores and array of lines and makes it easy to insert
// and delete in it
type LineArray struct {
	lines []Line
}

// Append efficiently appends lines together
// It allocates an additional 10000 lines if the original estimate
// is incorrect
func Append(slice []Line, data ...Line) []Line {
	l := len(slice)
	if l+len(data) > cap(slice) { // reallocate
		newSlice := make([]Line, (l+len(data))+10000)
		// The copy function is predeclared and works for any slice type.
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0 : l+len(data)]
	for i, c := range data {
		slice[l+i] = c
	}
	return slice
}

// reader can be thought as a file descriptor
// This is called when main initializes. Looks like also support loading from existing file
// NewLineArray returns a new line array from an array of bytes
func NewLineArray(size int64, reader io.Reader) *LineArray {
	la := new(LineArray)

	la.lines = make([]Line, 0, 1000) //a thousand line

	br := bufio.NewReader(reader)
	var loaded int

	n := 0
	for {
		data, err := br.ReadBytes('\n') // read until first occurrence of '\n'
		if len(data) > 1 && data[len(data)-2] == '\r' {
			data = append(data[:len(data)-2], '\n')
			if fileformat == 0 {
				fileformat = 2
			}
		} else if len(data) > 0 {
			if fileformat == 0 {
				fileformat = 1
			}
		}

		if n >= 1000 && loaded >= 0 {
			totalLinesNum := int(float64(size) * (float64(n) / float64(loaded)))
			newSlice := make([]Line, len(la.lines), totalLinesNum+10000)
			copy(newSlice, la.lines)
			la.lines = newSlice
			loaded = -1
		}

		if loaded >= 0 {
			loaded += len(data) // loaded store the amoung of bytes read so far
		}

		if err != nil {
			if err == io.EOF {
				la.lines = Append(la.lines, Line{data[:], nil, nil, false})
				// la.lines = Append(la.lines, Line{data[:len(data)]})
			}
			// Last line was read
			break
		} else {
			// la.lines = Append(la.lines, Line{data[:len(data)-1]})
			la.lines = Append(la.lines, Line{data[:len(data)-1], nil, nil, false}) // append
		}
		n++ // n stores the number of lines read so far
	}

	return la
}

// Returns the String representation of the LineArray
func (la *LineArray) String() string {
	str := ""
	for i, l := range la.lines {
		str += string(l.data)
		if i != len(la.lines)-1 {
			str += "\n" // need to add \n as it is not stored in lineArray
		}
	}
	return str
}

// SaveString returns the string that should be written to disk when
// the line array is saved
// It is the same as string but uses crlf or lf line endings depending
func (la *LineArray) SaveString(useCrlf bool) string {
	str := ""
	for i, l := range la.lines {
		str += string(l.data)
		if i != len(la.lines)-1 {
			if useCrlf {
				str += "\r"
			}
			str += "\n"
		}
	}
	return str
}

// NewlineBelow adds a newline below the given line number
func (la *LineArray) NewlineBelow(y int) {
	la.lines = append(la.lines, Line{[]byte{' '}, nil, nil, false}) // append one line, why ' '?
	copy(la.lines[y+2:], la.lines[y+1:])                            // shifting lines under
	la.lines[y+1] = Line{[]byte{}, la.lines[y].state, nil, false}   // ' ' disappeared..
}

// inserts a byte array at a given location
func (la *LineArray) insert(pos Loc, value []byte) {
	x, y := runeToByteIndex(pos.X, la.lines[pos.Y].data), pos.Y
	// x, y := pos.x, pos.y
	for i := 0; i < len(value); i++ { // go through each byte in value[]
		if value[i] == '\n' { // if user enters a return, then split
			la.Split(Loc{x, y}) // this is the underlying split mechanism for lines. calls insert
			x = 0
			y++
			continue
		}
		la.insertByte(Loc{x, y}, value[i])
		x++
	}
}

// inserts a byte at a given location, super-relavant
func (la *LineArray) insertByte(pos Loc, value byte) {
	la.lines[pos.Y].data = append(la.lines[pos.Y].data, 0) // appends an extra 0 at the end
	// shifting the chars on the right of value by 1 right
	copy(la.lines[pos.Y].data[pos.X+1:], la.lines[pos.Y].data[pos.X:]) // func copy(dst, src []Type) int
	la.lines[pos.Y].data[pos.X] = value
}

// JoinLines joins the two lines a and b
func (la *LineArray) JoinLines(a, b int) {
	la.insert(Loc{len(la.lines[a].data), a}, la.lines[b].data)
	la.DeleteLine(b)
}

// Split splits a line at a given position
func (la *LineArray) Split(pos Loc) {
	la.NewlineBelow(pos.Y)
	la.insert(Loc{0, pos.Y + 1}, la.lines[pos.Y].data[pos.X:])
	la.lines[pos.Y+1].state = la.lines[pos.Y].state
	la.lines[pos.Y].state = nil
	la.lines[pos.Y].match = nil
	la.lines[pos.Y+1].match = nil
	la.lines[pos.Y].rehighlight = true
	la.DeleteToEnd(Loc{pos.X, pos.Y}) // delete the characters on the righthand of the "\n" split
}

// removes from start to end, also forms the deleted substring
func (la *LineArray) remove(start, end Loc) string { // may also need to support for document
	sub := la.Substr(start, end)
	startX := runeToByteIndex(start.X, la.lines[start.Y].data)
	endX := runeToByteIndex(end.X, la.lines[end.Y].data)
	if start.Y == end.Y { // if a line is deleted in the middle, merge the gap
		la.lines[start.Y].data = append(la.lines[start.Y].data[:startX], la.lines[start.Y].data[endX:]...)
	} else { // the deleted region acrosses two lines, merge the two lines
		for i := start.Y + 1; i <= end.Y-1; i++ {
			la.DeleteLine(start.Y + 1)
		}
		la.DeleteToEnd(Loc{startX, start.Y})
		la.DeleteFromStart(Loc{endX - 1, start.Y + 1})
		la.JoinLines(start.Y, start.Y+1)
	}
	return sub
}

// DeleteToEnd deletes from the end of a line to the position
func (la *LineArray) DeleteToEnd(pos Loc) {
	la.lines[pos.Y].data = la.lines[pos.Y].data[:pos.X]
}

// DeleteFromStart deletes from the start of a line to the position
func (la *LineArray) DeleteFromStart(pos Loc) {
	la.lines[pos.Y].data = la.lines[pos.Y].data[pos.X+1:]
}

// DeleteLine deletes the line number
func (la *LineArray) DeleteLine(y int) {
	la.lines = la.lines[:y+copy(la.lines[y:], la.lines[y+1:])]
}

// DeleteByte deletes the byte at a position
func (la *LineArray) DeleteByte(pos Loc) { // This does not take into account of line changes
	la.lines[pos.Y].data = la.lines[pos.Y].data[:pos.X+copy(la.lines[pos.Y].data[pos.X:], la.lines[pos.Y].data[pos.X+1:])]
}

// Substr returns the string representation between two locations
func (la *LineArray) Substr(start, end Loc) string {
	startX := runeToByteIndex(start.X, la.lines[start.Y].data)
	endX := runeToByteIndex(end.X, la.lines[end.Y].data)
	if start.Y == end.Y {
		return string(la.lines[start.Y].data[startX:endX])
	}
	var str string
	str += string(la.lines[start.Y].data[startX:]) + "\n"
	for i := start.Y + 1; i <= end.Y-1; i++ {
		str += string(la.lines[i].data) + "\n"
	}
	str += string(la.lines[end.Y].data[:endX])
	return str
}

// State gets the highlight state for the given line number
func (la *LineArray) State(lineN int) highlight.State {
	return la.lines[lineN].state
}

// SetState sets the highlight state at the given line number
func (la *LineArray) SetState(lineN int, s highlight.State) {
	la.lines[lineN].state = s
}

// SetMatch sets the match at the given line number
func (la *LineArray) SetMatch(lineN int, m highlight.LineMatch) {
	la.lines[lineN].match = m
}

// Match retrieves the match for the given line number
func (la *LineArray) Match(lineN int) highlight.LineMatch {
	return la.lines[lineN].match
}
