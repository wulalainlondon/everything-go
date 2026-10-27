package core

import contract "everything-go/contracts/task-api/v1"

// pinTaskSchema is a one-connection transport contract, never a permission.
// No selector means the exact original contract. A changed selector retires
// Task API use on this connection, including already-bound slow operations.
func (c *Client) pinTaskSchema(hash string) bool {
	if hash == "" {
		hash = contract.Hash()
	}
	if c.taskSchemaRetired.Load() {
		return false
	}
	if c.taskSchemaHash.CompareAndSwap(nil, &hash) {
		return true
	}
	if current := c.taskSchemaHash.Load(); current == nil || *current != hash {
		c.taskSchemaRetired.Store(true)
		return false
	}
	return true
}
func (c *Client) currentTaskSchema() string {
	if hash := c.taskSchemaHash.Load(); hash != nil {
		return *hash
	}
	return contract.Hash()
}
