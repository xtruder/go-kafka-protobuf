package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/xtruder/go-kafka-protobuf/srclient"
)

func initSchemaRegistryFlags(cmd *cobra.Command) {
	cmd.Flags().String("registry-url", "", "Schema registry URL")
	cmd.MarkFlagRequired("registry-url")

	cmd.Flags().String("registry-credentials", "", "Schema registry credentials in format of 'user:pass'")

	cmd.Flags().Bool("registry-insecure", false, "Wheter insecure connections to schema registry are allowed")
}

func initSchemaRegistryClient(cmd *cobra.Command) (srclient.Client, error) {
	opts := []srclient.Option{}

	url, err := cmd.Flags().GetString("registry-url")
	if err != nil {
		return nil, err
	}

	opts = append(opts, srclient.WithURL(url))

	creds, err := cmd.Flags().GetString("registry-credentials")
	if err != nil {
		return nil, err
	}

	if creds != "" {
		credPair := strings.Split(creds, ":")

		if len(credPair) != 2 {
			return nil, fmt.Errorf("invalid format for schema registry credentials, must be 'user:pass'")
		}

		opts = append(opts, srclient.WithCredentials(credPair[0], credPair[1]))
	}

	insecure, err := cmd.Flags().GetBool("registry-insecure")
	if err != nil {
		return nil, err
	}

	opts = append(opts, srclient.WithInsecure(insecure))

	return srclient.NewClient(opts...), nil
}
