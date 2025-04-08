package cmd

import (
	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)

var (
	groupId string
	topics  []string
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from kafka",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := getConfig(cmd.Context())
		return client.NewKafkaClient(conf).Consume(groupId, topics...)
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().StringVarP(&groupId, "group", "g", "default_group", "set group id")
	consumeCmd.Flags().StringSliceVarP(&topics, "topics", "t", []string{}, "topic name")
}
