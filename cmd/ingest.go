package cmd

import (
	"verifier/pkg/ingest"
	"verifier/pkg/utils"

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
		batchSize, _ := cmd.Flags().GetInt("batchSize")
		indexPrefix, _ := cmd.Flags().GetString("indexPrefix")
		numIndices, _ := cmd.Flags().GetInt("numIndices")
		dataFile, _ := cmd.Flags().GetString("data")

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v\n", totalEvents)
		log.Infof("batchSize : %+v\n", batchSize)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("numIndices : %+v\n", numIndices)
		reader, err := getReaderFromArgs(dataFile)
		if err != nil {
			log.Fatalf("Failed to initalize reader! %v", err)
		}

		ingest.StartIngestion(reader, totalEvents, batchSize, dest, indexPrefix, numIndices, processCount)
	},
}

func getReaderFromArgs(str string) (utils.Reader, error) {
	var rdr utils.Reader
	if str == "" {
		log.Infof("Initializing static reader")
		rdr = &utils.StaticReader{}
	} else {
		log.Infof("Initializing file reader from %s", str)
		rdr = &utils.FileReader{}
	}
	err := rdr.Init(str)
	return rdr, err
}

func init() {
	rootCmd.AddCommand(ingestCmd)
	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from.")
	ingestCmd.PersistentFlags().StringP("dest", "d", "", "Destination URL")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 100, "Batch size")
	ingestCmd.PersistentFlags().StringP("indexPrefix", "i", "ind", "index prefix")
	ingestCmd.PersistentFlags().IntP("numIndices", "n", 1, "number of indices to ingest to")
	ingestCmd.PersistentFlags().String("data", "", "path to json file to use as sent events")
}
