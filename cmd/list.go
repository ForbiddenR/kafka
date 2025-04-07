/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Get topic list from kafka but system topics",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		config := getConfig(cmd.Context())
		return client.NewKafkaClient(config).List()
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}
