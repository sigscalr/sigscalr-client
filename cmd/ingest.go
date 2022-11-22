package cmd

import (
	"fmt"
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
		generatorType, _ := cmd.Flags().GetString("generator")
		dataFile, _ := cmd.Flags().GetString("filePath")

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v\n", totalEvents)
		log.Infof("batchSize : %+v\n", batchSize)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("numIndices : %+v\n", numIndices)
		log.Infof("generatorType : %+v\n", generatorType)
		reader, err := getReaderFromArgs(generatorType, dataFile)
		if err != nil {
			log.Fatalf("Failed to initalize reader! %v", err)
		}

		ingest.StartIngestion(reader, totalEvents, batchSize, dest, indexPrefix, numIndices, processCount)
	},
}

func getReaderFromArgs(gentype, str string) (utils.Reader, error) {
	var rdr utils.Reader
	switch gentype {
	case "", "static":
		log.Infof("Initializing static reader")
		rdr = &utils.StaticReader{}
	case "dynamic":
		rdr = &utils.DynamicReader{}
	case "file":
		log.Infof("Initializing file reader from %s", str)
		rdr = &utils.FileReader{}
	default:
		return nil, fmt.Errorf("unsupported reader type %s. Options=[static,dynamic,file]", gentype)
	}
	err := rdr.Init(str)
	return rdr, err
}

func init() {
	rootCmd.AddCommand(ingestCmd)
	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from.")
	ingestCmd.PersistentFlags().StringP("dest", "d", "", "Destination URL. Client will append /bulk")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events to send")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 100, "Batch size")
	ingestCmd.PersistentFlags().StringP("indexPrefix", "i", "ind", "index prefix")
	ingestCmd.PersistentFlags().IntP("numIndices", "n", 1, "number of indices to ingest to")
	ingestCmd.PersistentFlags().StringP("generator", "g", "static", "type of generator to use. Options=[static,dynamic,file]. If file is selected, -x/--filePath must be specified")
	ingestCmd.PersistentFlags().StringP("filePath", "x", "", "path to json file to use as logs")
}
