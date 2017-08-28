package keyspace

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/uol/gobol"
)

// Config holds the keyspace information
type Config struct {
	Key               string `json:"key"`
	Name              string `json:"name"`
	Datacenter        string `json:"datacenter"`
	ReplicationFactor int    `json:"replicationFactor"`
	Contact           string `json:"contact"`
	TTL               int    `json:"ttl"`
	TUUID             bool   `json:"tuuid"`
}

// Validate checks if the config is sane
func (c *Config) Validate() gobol.Error {
	if c.Datacenter == "" {
		return errValidationS("CreateKeyspace", "Datacenter can not be empty or nil")
	}

	if c.ReplicationFactor <= 0 || c.ReplicationFactor > 3 {
		return errValidationS(
			"CreateKeyspace",
			"Replication factor can not be less than or equal to 0 or greater than 3",
		)
	}

	if !govalidator.IsEmail(c.Contact) {
		return errValidationS("CreateKeyspace", "Contact field should be a valid email address")
	}

	if c.TTL <= 0 {
		return errValidationS("CreateKeyspace", `TTL can not be less or equal to zero`)
	}
	if c.TTL > maxTTL {
		return errValidationS("CreateKeyspace", fmt.Sprintf(`Max TTL allowed is %v`, maxTTL))
	}
	return nil
}

// Validate checks if ConfigUpdate is sane
func (c *ConfigUpdate) Validate() gobol.Error {

	if !govalidator.IsEmail(c.Contact) {
		return errValidationS("CreateKeyspace", "Contact field should be a valid email address")
	}

	if !validKey.MatchString(c.Name) {
		return errValidationS(
			"CreateKeyspace",
			`Wrong Format: Field "name" is not well formed. NO information will be saved`,
		)
	}

	return nil
}

// ConfigUpdate is a configuration update request
type ConfigUpdate struct {
	Name    string `json:"name"`
	Contact string `json:"contact"`
}

// CreateResponse is the ReST response for a keyspace creation API call
type CreateResponse struct {
	Ksid string `json:"ksid,omitempty"`
}

// Response is a generic response for the ReST API
type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}
