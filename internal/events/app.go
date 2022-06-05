package events

type AppStop struct{}

func NewAppStop() AppStop {
	return AppStop{}
}

func (e AppStop) Code() string {
	return "app.stop"
}
