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
and usage of using your command.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := getConfig(cmd.Context())
		return client.NewKafkaClient(conf).CreateTopic(args[0], partitionNum)
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.Flags().Int32VarP(&partitionNum, "partition", "p", 1, "partition number")
}
