package cmd

import (
	log "github.com/sirupsen/logrus"

	verifier "verifier/pkg/verifier"

	"github.com/spf13/cobra"
)

// ingestCmd represents the ingest command
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "ingest records to SigScalr",
	Run: func(cmd *cobra.Command, args []string) {
		processCount, _ := cmd.Flags().GetInt("processCount")
		dest, _ := cmd.Flags().GetString("dest")
		totalEvents, _ := cmd.Flags().GetInt("totalEvents")
		batchSize, _ := cmd.Flags().GetInt("batchSize")
		indexName, _ := cmd.Flags().GetString("indexName")

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v\n", totalEvents)
		log.Infof("batchSize : %+v\n", batchSize)
		log.Infof("indexName : %+v\n", indexName)

		verifier.StartIngestion(totalEvents, batchSize, dest, indexName, processCount)
	},
}

func init() {
	rootCmd.AddCommand(ingestCmd)
	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from.")
	ingestCmd.PersistentFlags().StringP("dest", "d", "", "Destination URL")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 100, "Batch size")
	ingestCmd.PersistentFlags().StringP("indexName", "i", "index", "index name")
}
