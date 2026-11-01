package agentactivity

import (
	"bufio"
	"errors"
	"io"
)

// ReadRecords reads bounded newline-delimited body records from a caller-owned
// reader. Source identity and the header are the original owner's concern.
// Every rejected record and incomplete tail stays visible in safe counters.
func (p *HostedQualifier) ReadRecords(source io.Reader) error {
	r := bufio.NewReaderSize(source, MaxQualificationRecord+1)
	for {
		line, err := r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			p.q.Records++
			p.q.OversizedRecords++
			total := int64(len(line))
			last := byte(0)
			for errors.Is(err, bufio.ErrBufferFull) {
				line, err = r.ReadSlice('\n')
				total += int64(len(line))
				if len(line) > 0 {
					last = line[len(line)-1]
				}
			}
			if errors.Is(err, io.EOF) && last != '\n' {
				p.q.PartialTailRecords++
				p.q.PartialTailBytes += total
			}
		} else if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				p.q.Records++
				p.q.PartialTailRecords++
				p.q.PartialTailBytes += int64(len(line))
			} else if rejected := p.Observe(line); rejected != nil && !isRejection(rejected) {
				return rejected
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
