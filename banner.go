package main

import (
	"fmt"
	"strings"
)

// The MIGHTY wordmark in a six-row block font. Each glyph is a list of
// equal-width rows, joined column-wise at runtime so the letters stay aligned.
var mightyGlyphs = [][]string{
	{ // M
		"██   ██",
		"███ ███",
		"██ █ ██",
		"██   ██",
		"██   ██",
		"██   ██",
	},
	{ // I
		"████",
		" ██ ",
		" ██ ",
		" ██ ",
		" ██ ",
		"████",
	},
	{ // G
		" ██████",
		"██     ",
		"██  ███",
		"██   ██",
		"██   ██",
		" ██████",
	},
	{ // H
		"██   ██",
		"██   ██",
		"███████",
		"██   ██",
		"██   ██",
		"██   ██",
	},
	{ // T
		"███████",
		"   ██  ",
		"   ██  ",
		"   ██  ",
		"   ██  ",
		"   ██  ",
	},
	{ // Y
		"██   ██",
		" ██ ██ ",
		"  ███  ",
		"   ██  ",
		"   ██  ",
		"   ██  ",
	},
}

// Gradient running from white at the top to purple at the bottom (256-color).
var bannerGradient = []string{"231", "189", "183", "141", "135", "99"}

// mightyBanner assembles the glyphs into six printable rows.
func mightyBanner() []string {
	const h = 6
	rows := make([]string, h)
	for i := 0; i < h; i++ {
		parts := make([]string, len(mightyGlyphs))
		for g, glyph := range mightyGlyphs {
			parts[g] = glyph[i]
		}
		rows[i] = strings.Join(parts, "  ")
	}
	return rows
}

// printBanner draws the wordmark with its gradient, the drip row, and the subtitle.
func printBanner() {
	rows := mightyBanner()
	width := 0
	for _, r := range rows {
		if n := runeWidth(r); n > width {
			width = n
		}
	}

	fmt.Println()
	for i, r := range rows {
		if colorOn {
			fmt.Println("  " + paint("38;5;"+bannerGradient[i], r))
		} else {
			fmt.Println("  " + r)
		}
	}

	// A light drip row echoing the original logo treatment.
	drip := drips(width)
	if colorOn {
		fmt.Println("  " + paint("38;5;99", drip))
	} else {
		fmt.Println("  " + drip)
	}

	sub := "A U T O   C H E C K E R   •   instagram usernames"
	fmt.Println("  " + cGray(sub))
	fmt.Println()
}

// drips builds a row of fade glyphs as wide as the banner.
func drips(width int) string {
	if width <= 0 {
		return ""
	}
	glyphs := []rune{'▓', '▒', '░', '│', ' ', '░', '▒'}
	var b strings.Builder
	for i := 0; i < width; i++ {
		b.WriteRune(glyphs[i%len(glyphs)])
	}
	return b.String()
}

// runeWidth counts runes, not bytes, in a line.
func runeWidth(s string) int {
	return len([]rune(s))
}
