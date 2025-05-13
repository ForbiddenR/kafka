package cmd

import (
	"os"
	"strings"

	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)

var statisticsCmd = &cobra.Command{
	Use:   "statistics",
	Short: "Produce statistics messages to kafka",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := getConfig(cmd.Context())
		hostname, _ := os.Hostname()
		slices := strings.Split(hostname, "-")
		return client.NewKafkaClient(conf).Produce(slices[len(slices)-1], args[0])
	},
}

func init() {
	rootCmd.AddCommand(statisticsCmd)
}
