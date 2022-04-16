package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(level zapcore.Level, traceLevel zapcore.Level, name string, version string, instanceID string, env string) (*zap.Logger, error) {
	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		NameKey:        "app",
		LevelKey:       "level",
		TimeKey:        "time",
		CallerKey:      "caller",
		FunctionKey:    "function",
		MessageKey:     "msg",
		StacktraceKey:  "trace",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	})

	return zap.New(
		zapcore.NewCore(encoder, os.Stdout, level),
		zap.AddStacktrace(traceLevel),
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.Fields(
			zap.String("version", version),
			zap.String("app", name),
			zap.String("instance_id", instanceID),
			zap.String("env", env),
		),
	), nil
}
