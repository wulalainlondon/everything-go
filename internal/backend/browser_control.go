package backend

// Browser guidance is provided by the installed, authenticated local service.
type BrowserControlInstructions interface{ BrowserControlInstructions() string }
