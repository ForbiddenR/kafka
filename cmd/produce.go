package cmd

import (
	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to kafka",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := getConfig(cmd.Context())
		return client.NewKafkaClient(conf).Produce(topic)
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic name")
}
