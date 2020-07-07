package object

func (e Float16) Uint16() uint16 {
	return e.Value().Uint16()
}

func (e Float16) tof32() float32 {
	return e.Value().Float32()
}
