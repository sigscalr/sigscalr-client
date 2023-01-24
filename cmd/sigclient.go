package cmd

import (
	"verifier/pkg/ingest"
	"verifier/pkg/query"

	log "github.com/sirupsen/logrus"

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
		continuous, _ := cmd.Flags().GetBool("continuous")
		batchSize, _ := cmd.Flags().GetInt("batchSize")
		indexPrefix, _ := cmd.Flags().GetString("indexPrefix")
		numIndices, _ := cmd.Flags().GetInt("numIndices")
		generatorType, _ := cmd.Flags().GetString("generator")
		ts, _ := cmd.Flags().GetBool("timestamp")
		dataFile, _ := cmd.Flags().GetString("filePath")

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v. Continuous: %+v\n", totalEvents, continuous)
		log.Infof("batchSize : %+v\n", batchSize)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("numIndices : %+v\n", numIndices)
		log.Infof("generatorType : %+v. Add timestamp: %+v\n", generatorType, ts)
		ingest.StartIngestion(generatorType, dataFile, totalEvents, continuous, batchSize, dest, indexPrefix, numIndices, processCount, ts)
	},
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "send queries to SigScalr",
	Run: func(cmd *cobra.Command, args []string) {
		dest, _ := cmd.Flags().GetString("dest")
		numIterations, _ := cmd.Flags().GetInt("numIterations")
		verbose, _ := cmd.Flags().GetBool("verbose")
		continuous, _ := cmd.Flags().GetBool("continuous")
		indexPrefix, _ := cmd.Flags().GetString("indexPrefix")

		log.Infof("dest : %+v\n", dest)
		log.Infof("numIterations : %+v\n", numIterations)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("verbose : %+v\n", verbose)
		log.Infof("continuous : %+v\n", continuous)
		query.StartQuery(dest, numIterations, indexPrefix, continuous, verbose)
	},
}

func init() {
	rootCmd.PersistentFlags().StringP("dest", "d", "", "ES Server URL.")
	rootCmd.PersistentFlags().StringP("indexPrefix", "i", "ind", "index prefix")

	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from.")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events to send")
	ingestCmd.Flags().BoolP("continuous", "c", false, "Continous ingestion will ingore -t and will constantly send events as fast as possible")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 100, "Batch size")
	ingestCmd.Flags().BoolP("timestamp", "s", false, "Add timestamp in payload")
	ingestCmd.PersistentFlags().IntP("numIndices", "n", 1, "number of indices to ingest to")
	ingestCmd.PersistentFlags().StringP("generator", "g", "static", "type of generator to use. Options=[static,dynamic,file]. If file is selected, -x/--filePath must be specified")
	ingestCmd.PersistentFlags().StringP("filePath", "x", "", "path to json file to use as logs")

	queryCmd.PersistentFlags().IntP("numIterations", "n", 10, "number of times to run entire query suite")
	queryCmd.Flags().BoolP("verbose", "v", false, "Verbose querying will output raw docs returned by queries")
	queryCmd.Flags().BoolP("continuous", "c", false, "Continuous querying will ignore -c and -v and will continuously send queries to the destination")

	rootCmd.AddCommand(ingestCmd)
	rootCmd.AddCommand(queryCmd)
}
