//go:build ignore

package core

import (
	"encoding/json"
	"strings"
)

const (
	clear  = "\x1b[0m"
	bright = "\x1b[1m"
	gray   = "\x1b[90m"
	yellow = "\x1b[33m"
)

// Command represents a Tile38 command.
type Command struct {
	Name       string     `json:"-"`
	Summary    string     `json:"summary"`
	Complexity string     `json:"complexity"`
	Arguments  []Argument `json:"arguments"`
	Since      string     `json:"since"`
	Group      string     `json:"group"`
	DevOnly    bool       `json:"dev"`
}

// String returns a string representation of the command.
func (c Command) String() string {
	var s = c.Name
	for _, arg := range c.Arguments {
		s += " " + arg.String()
	}
	return s
}

// TermOutput returns a string representation of the command suitable for displaying in a terminal.
func (c Command) TermOutput(indent string) string {
	line := c.String()
	var line1 string
	if strings.HasPrefix(line, c.Name) {
		line1 = bright + c.Name + clear + gray + line[len(c.Name):] + clear
	} else {
		line1 = bright + strings.Replace(c.String(), " ", " "+clear+gray, 1) + clear
	}
	line2 := yellow + "summary: " + clear + c.Summary
	//line3 := yellow + "since: " + clear + c.Since
	return indent + line1 + "\n" + indent + line2 + "\n" //+ indent + line3 + "\n"
}

// EnumArg represents a enum arguments.
type EnumArg struct {
	Name      string     `json:"name"`
	Arguments []Argument `json:"arguments"`
}

// String returns a string representation of an EnumArg.
func (a EnumArg) String() string {
	var s = a.Name
	for _, arg := range a.Arguments {
		s += " " + arg.String()
	}
	return s
}

// Argument represents a command argument.
type Argument struct {
	Command  string      `json:"command"`
	NameAny  interface{} `json:"name"`
	TypeAny  interface{} `json:"type"`
	Optional bool        `json:"optional"`
	Multiple bool        `json:"multiple"`
	Variadic bool        `json:"variadic"`
	Enum     []string    `json:"enum"`
	EnumArgs []EnumArg   `json:"enumargs"`
}

// String returns a string representation of an Argument.
func (a Argument) String() string {
	var s string
	if a.Command != "" {
		s += " " + a.Command
	}
	if len(a.EnumArgs) > 0 {
		eargs := ""
		for _, arg := range a.EnumArgs {
			v := arg.String()
			if strings.Contains(v, " ") {
				v = "(" + v + ")"
			}
			eargs += v + "|"
		}
		if len(eargs) > 0 {
			eargs = eargs[:len(eargs)-1]
		}
		s += " " + eargs
	} else if len(a.Enum) > 0 {
		s += " " + strings.Join(a.Enum, "|")
	} else {
		names, _ := a.NameTypes()
		subs := ""
		for _, name := range names {
			subs += " " + name
		}
		subs = strings.TrimSpace(subs)
		s += " " + subs
		if a.Variadic {
			if len(names) == 0 {
				s += " [" + subs + " ...]"
			} else {
				s += " [" + names[len(names)-1] + " ...]"
			}
		}
		if a.Multiple {
			s += " ..."
		}
	}
	s = strings.TrimSpace(s)
	if a.Optional {
		s = "[" + s + "]"
	}
	return s
}

func parseAnyStringArray(any interface{}) []string {
	if str, ok := any.(string); ok {
		return []string{str}
	} else if any, ok := any.([]interface{}); ok {
		arr := []string{}
		for _, any := range any {
			if str, ok := any.(string); ok {
				arr = append(arr, str)
			}
		}
		return arr
	}
	return []string{}
}

// NameTypes returns the types and names of an argument as separate arrays.
func (a Argument) NameTypes() (names, types []string) {
	names = parseAnyStringArray(a.NameAny)
	types = parseAnyStringArray(a.TypeAny)
	if len(types) > len(names) {
		types = types[:len(names)]
	} else {
		for len(types) < len(names) {
			types = append(types, "")
		}
	}
	return
}

// Commands is a map of all of the commands.
var Commands = func() map[string]Command {
	var commands map[string]Command
	if err := json.Unmarshal([]byte(commandsJSON), &commands); err != nil {
		panic(err.Error())
	}
	for name, command := range commands {
		command.Name = strings.ToUpper(name)
		commands[name] = command
	}
	return commands
}()

var commandsJSON = `{{.CommandsJSON}}`
