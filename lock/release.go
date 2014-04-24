package lock

import "github.com/juju/errgo"

func (l *Lock) Release() error {
	if l == nil {
		return errgo.New("nil lock")
	}

	_, err := l.client.Delete(l.key, false)
	if err != nil {
		return errgo.Mask(err)
	}

	return nil
}
