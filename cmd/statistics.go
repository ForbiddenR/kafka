package cmd

import (
	"os"
	"strings"

	"github.com/ForbiddenR/kafka/client/internal/client"
	"github.com/spf13/cobra"
)


var statisticsCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to kafka",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := getConfig(cmd.Context())
		if prefix == "" {
			hostname, _ := os.Hostname()
			hs := strings.Split(hostname, "-")
			prefix = hs[len(hs)-1]
		}
		return client.NewKafkaClient(conf).Produce(prefix, args[0])
	},
}

func init() {
	rootCmd.AddCommand(statisticsCmd)
	// produceCmd.Flags().StringVarP(&prefix, "prefix", "p", "default_prefix", "message prefix")
}
