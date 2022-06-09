package uuid

import uuid2 "github.com/google/uuid"

var NullUUID = uuid2.UUID{}

func NewUUIDAsString() string {
	return uuid2.New().String()
}

func New() uuid2.UUID {
	return uuid2.New()
}
