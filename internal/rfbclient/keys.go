package rfbclient

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// TextKeys maps Unicode text to X11 keysyms, without a clipboard or command.
func TextKeys(text string) ([]uint32, error) {
	if !utf8.ValidString(text) {
		return nil, fmt.Errorf("text must be UTF-8")
	}
	result := make([]uint32, 0, len(text))
	for _, r := range text {
		switch r {
		case '\n', '\r':
			result = append(result, 0xff0d)
		case '\t':
			result = append(result, 0xff09)
		default:
			if r < 32 || r == 127 {
				return nil, fmt.Errorf("unsupported control character")
			}
			if r <= 255 {
				result = append(result, uint32(r))
			} else {
				result = append(result, 0x01000000|uint32(r))
			}
		}
	}
	return result, nil
}

// ComboKeys returns modifiers first and the final key last. Callers release
// these keys in reverse order, including modifiers, before closing the call.
func ComboKeys(combo string) ([]uint32, error) {
	parts := strings.Split(combo, "+")
	keys := make([]uint32, 0, len(parts))
	used := map[uint32]bool{}
	modifiers := map[string]uint32{"ctrl": 0xffe3, "control": 0xffe3, "shift": 0xffe1, "alt": 0xffe9, "super": 0xffeb, "meta": 0xffe7}
	for _, part := range parts[:len(parts)-1] {
		key, ok := modifiers[strings.ToLower(part)]
		if !ok || used[key] {
			return nil, fmt.Errorf("unknown or repeated key modifier %q", part)
		}
		used[key] = true
		keys = append(keys, key)
	}
	name := parts[len(parts)-1]
	named := map[string]uint32{"return": 0xff0d, "enter": 0xff0d, "tab": 0xff09, "escape": 0xff1b, "esc": 0xff1b, "backspace": 0xff08, "delete": 0xffff, "insert": 0xff63, "home": 0xff50, "end": 0xff57, "pageup": 0xff55, "pagedown": 0xff56, "left": 0xff51, "up": 0xff52, "right": 0xff53, "down": 0xff54, "space": 32, "plus": 43}
	if key, ok := named[strings.ToLower(name)]; ok {
		return append(keys, key), nil
	}
	if n, err := strconv.Atoi(strings.TrimPrefix(strings.ToLower(name), "f")); err == nil && strings.HasPrefix(strings.ToLower(name), "f") && n >= 1 && n <= 12 {
		return append(keys, uint32(0xffbd+n)), nil
	}
	if utf8.RuneCountInString(name) == 1 {
		value, err := TextKeys(name)
		if err == nil {
			return append(keys, value...), nil
		}
	}
	return nil, fmt.Errorf("unknown key %q", name)
}
