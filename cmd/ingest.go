package cmd

import (
	"fmt"

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
		timeRange, _ := cmd.Flags().GetInt("timeRange")
		filePrefix, _ := cmd.Flags().GetString("filePrefix")

		fmt.Printf("processCount : %+v\n", processCount)
		fmt.Printf("dest : %+v\n", dest)
		fmt.Printf("totalEvents : %+v\n", totalEvents)
		fmt.Printf("batchSize : %+v\n", batchSize)
		fmt.Printf("indexName : %+v\n", indexName)
		fmt.Printf("timeRange : %+v\n", timeRange)
		fmt.Printf("filePrefix : %+v\n", filePrefix)

		verifier.StartIngestion(totalEvents, batchSize, dest, indexName, filePrefix, processCount, timeRange)
	},
}

func init() {
	rootCmd.AddCommand(ingestCmd)
	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from different files.")
	ingestCmd.PersistentFlags().StringP("dest", "d", "", "Destination URL")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 1000, "Batch size")
	ingestCmd.PersistentFlags().StringP("indexName", "i", "default", "index name ")
	ingestCmd.PersistentFlags().IntP("timeRange", "r", 3600, "Time Duration is seconds for event range")
	ingestCmd.PersistentFlags().StringP("filePrefix", "x", "ab", "file prefix to read the events from to ingest engine")
}
