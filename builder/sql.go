package builder

type sqler struct {
	query expr
}

func (b *sqler) Build() (string, []interface{}, error) {
	if _, err := b.query.build(1); err != nil {
		return "", nil, err
	}
	return b.query.text, b.query.params, nil
}
