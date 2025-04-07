/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)

var (
	topic        string
	partitionNum int32
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Crate a topic in kafka",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		config := getConfig(cmd.Context())
		return client.NewKafkaClient(config).CreateTopic(topic, partitionNum)
	},
}

func init() {
	rootCmd.AddCommand(createCmd)

	createCmd.Flags().StringVarP(&topic, "topic", "t", "topic", "topic name")
	createCmd.Flags().Int32VarP(&partitionNum, "partition", "p", 1, "partition number")
}
