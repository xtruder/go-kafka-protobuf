/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc/protoprint"
	"github.com/spf13/cobra"

	"github.com/xtruder/go-kafka-protobuf/protobuf"
	"github.com/xtruder/go-kafka-protobuf/srclient"
)

// genschemaCmd represents the genschema command
var genschemaCmd = &cobra.Command{
	Use:   "genschema",
	Short: "Generates schema from schema registry",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, err := cmd.Flags().GetString("name")
		if err != nil {
			return err
		}

		topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			return err
		}

		dest, err := cmd.Flags().GetString("dest")
		if err != nil {
			return err
		}

		fmt.Printf("generating schema for topic '%s' into '%s'\n", topic, dest)

		client, err := initSchemaRegistryClient(cmd)
		if err != nil {
			return err
		}

		if err := genSchema(cmd.Context(), client, name, topic, dest); err != nil {
			return err
		}

		fmt.Println("schema successfully generated")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(genschemaCmd)

	genschemaCmd.Flags().StringP("name", "n", "schema.proto", "Name of the schema")

	genschemaCmd.Flags().StringP("dest", "d", "", "Destination schema output path")
	genschemaCmd.MarkFlagRequired("dest")

	genschemaCmd.Flags().StringP("topic", "t", "", "Topic to generate schema for")
	genschemaCmd.MarkFlagRequired("topic")

	initSchemaRegistryFlags(genschemaCmd)
}

func genSchema(ctx context.Context, client srclient.Client,
	name string, topic string, dest string) error {
	schema, err := client.GetLatestSchema(ctx, topic)
	if err != nil {
		return err
	}

	if schema.Type == srclient.ProtobufSchemaType {
		registrator := protobuf.NewSchemaRegistrator(client)
		desc, err := registrator.Load(ctx, schema.ID, name)
		if err != nil {
			return err
		}

		printer := protoprint.Printer{}
		if err := printer.PrintProtosToFileSystem(desc, dest); err != nil {
			return err
		}
	}

	return nil
}
