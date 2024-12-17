package logger

import (
	"order-app/config"

	"go.uber.org/zap"
)

func NewZapLogger() (*zap.Logger, error) {
	zapCfg := config.NewZapLoggerConfig()
	return zapCfg.Build()
}
