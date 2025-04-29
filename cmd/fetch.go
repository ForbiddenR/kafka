package cmd

import (
	"time"

	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)

var (
	key   string
	since string
)

// deleteCmd represents the delete command
var fetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch message from topic by target key from kafka",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dur, err := time.ParseDuration(since)
		if err != nil {
			return err
		}
		conf := getConfig(cmd.Context())
		return client.NewKafkaClient(conf).Fetch(key, topic, dur)
	},
}

func init() {
	rootCmd.AddCommand(fetchCmd)
	fetchCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic name")
	fetchCmd.Flags().StringVarP(&key, "key", "k", "", "key name")
	fetchCmd.Flags().StringVarP(&since, "since", "s", "24h", "since time")
}
