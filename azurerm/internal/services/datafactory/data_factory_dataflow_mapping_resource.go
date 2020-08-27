package datafactory

import (
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/datafactory/mgmt/2018-06-01/datafactory"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmDataFactoryDataFlowMapping() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmDataFactoryMappingDataFlowCreateUpdate,
		Read:   resourceArmDataFactoryMappingDataFlowRead,
		Update: resourceArmDataFactoryMappingDataFlowCreateUpdate,
		Delete: resourceArmDataFactoryMappingDataFlowDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(30 * time.Minute),
			Read:   schema.DefaultTimeout(5 * time.Minute),
			Update: schema.DefaultTimeout(30 * time.Minute),
			Delete: schema.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateAzureRMDataFactoryDataFlowName,
			},

			// There's a bug in the Azure API where this is returned in lower-case
			// BUG: https://github.com/Azure/azure-rest-api-specs/issues/5788
			"resource_group_name": azure.SchemaResourceGroupNameDiffSuppress(),

			"data_factory_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.DataFactoryName(),
			},

			"source": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
						},
						"dataset_name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
						},
					},
				},
			},

			"sink": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
						},
						"dataset_name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
						},
					},
				},
			},

			"transformation": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
						},
					},
				},
			},

			"script": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"annotations": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validation.StringIsNotEmpty,
				},
			},

			"description": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"folder": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
		},
	}
}

func resourceArmDataFactoryMappingDataFlowCreateUpdate(d *schema.ResourceData, meta interface{}) error {

	client := meta.(*clients.Client).DataFactory.DataFlowsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	log.Printf("[INFO] preparing arguments for Data Factory Mapping Data Flow creation.")

	resourceGroupName := d.Get("resource_group_name").(string)
	name := d.Get("name").(string)
	dataFactoryName := d.Get("data_factory_name").(string)

	if d.IsNewResource() {
		existing, err := client.Get(ctx, resourceGroupName, dataFactoryName, name, "")
		if err != nil {
			if !utils.ResponseWasNotFound(existing.Response) {
				return fmt.Errorf("Error checking for presence of existing Data Factory Mapping Data Flow %q (Resource Group %q / Data Factory %q): %s", name, resourceGroupName, dataFactoryName, err)
			}
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_data_factory_dataflow_mapping", *existing.ID)
		}
	}

	dataSources := d.Get("source").([]interface{})
	dataSinks := d.Get("sink").([]interface{})
	transformations := d.Get("transformation").([]interface{})
	script := d.Get("script").(string)

	fmt.Print(dataSources)
	fmt.Print(dataSinks)

	loadedDataSources := expandDataFactoryMappingDataFlowSources(dataSources)
	loadedDataSinks := expandDataFactoryMappingDataFlowSinks(dataSinks)
	loadedTransformations := expandDataFactoryMappingDataFlowTransformations(transformations)

	props := &datafactory.MappingDataFlowTypeProperties{
		Sources:         &loadedDataSources,
		Sinks:           &loadedDataSinks,
		Transformations: &loadedTransformations,
		Script:          &script,
	}

	mappingDataFlow := &datafactory.MappingDataFlow{
		MappingDataFlowTypeProperties: props,
	}

	if v, ok := d.GetOk("annotations"); ok {
		annotations := v.([]interface{})
		mappingDataFlow.Annotations = &annotations
	}

	dataFlow := datafactory.DataFlowResource{
		Properties: mappingDataFlow,
	}

	if _, err := client.CreateOrUpdate(ctx, resourceGroupName, dataFactoryName, name, dataFlow, ""); err != nil {
		return fmt.Errorf("Error creating Data Factory Mapping Data Flow %q (Resource Group %q / Data Factory %q): %+v", name, resourceGroupName, dataFactoryName, err)
	}

	read, err := client.Get(ctx, resourceGroupName, dataFactoryName, name, "")
	if err != nil {
		return fmt.Errorf("Error retrieving Data Factory Mapping Data Flow %q (Resource Group %q / Data Factory %q): %+v", name, resourceGroupName, dataFactoryName, err)
	}

	if read.ID == nil {
		return fmt.Errorf("Cannot read Data Factory Mapping Data Flow %q (Resource Group %q / Data Factory %q) ID", name, resourceGroupName, dataFactoryName)
	}

	d.SetId(*read.ID)

	return resourceArmDataFactoryMappingDataFlowRead(d, meta)
}

func resourceArmDataFactoryMappingDataFlowRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).DataFactory.DataFlowsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := azure.ParseAzureResourceID(d.Id())
	if err != nil {
		return err
	}
	fmt.Print(id.Path)
	dataFactoryName := id.Path["factories"]
	dataflowName := id.Path["dataflows"]

	resp, err := client.Get(ctx, id.ResourceGroup, dataFactoryName, dataflowName, "")
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			d.SetId("")
			log.Printf("[DEBUG] Data Factory Mapping Data Flow %q was not found in Resource Group %q - removing from state!", dataflowName, id.ResourceGroup)
			return nil
		}
		return fmt.Errorf("Error reading the state of Data Factory Mapping Data Flow %q: %+v", dataflowName, err)
	}

	d.Set("name", resp.Name)
	d.Set("resource_group_name", id.ResourceGroup)
	d.Set("data_factory_name", dataFactoryName)

	dataFlowProps, ok := resp.Properties.AsMappingDataFlow()
	if !ok {
		return fmt.Errorf("Error classifiying Data Factory Mapping Data Flow %q (Data Factory %q / Resource Group %q): Expected: %q Received: %q", dataflowName, dataFactoryName, id.ResourceGroup, datafactory.TypeMappingDataFlow, *resp.Type)
	}

	if dataFlowProps != nil {

		if err := d.Set("sources", flattenDataFactoryMappingDataFlowSources(*dataFlowProps.Sources)); err != nil {
			return fmt.Errorf("setting `sources`: %s", err)
		}

		if err := d.Set("sinks", flattenDataFactoryMappingDataFlowSinks(*dataFlowProps.Sinks)); err != nil {
			return fmt.Errorf("setting `sinks`: %s", err)
		}

		if err := d.Set("transformations", flattenDataFactoryMappingDataFlowTransformations(*dataFlowProps.Transformations)); err != nil {
			return fmt.Errorf("setting `transformations`: %s", err)
		}

		annotations := flattenDataFactoryAnnotations(dataFlowProps.Annotations)
		if err := d.Set("annotations", annotations); err != nil {
			return fmt.Errorf("Error setting `annotations`: %+v", err)
		}

		if folder := dataFlowProps.Folder; folder != nil {
			if folder.Name != nil {
				d.Set("folder", folder.Name)
			}
		}

		if dataFlowProps.Description != nil {
			d.Set("description", dataFlowProps.Description)
		}
	}

	return nil
}

func resourceArmDataFactoryMappingDataFlowDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).DataFactory.DataFlowsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := azure.ParseAzureResourceID(d.Id())
	if err != nil {
		return err
	}
	dataFactoryName := id.Path["factories"]
	dataflowName := id.Path["dataflows"]

	if _, err = client.Delete(ctx, id.ResourceGroup, dataFactoryName, dataflowName); err != nil {
		return fmt.Errorf("Error deleting Data Factory Mapping Data Flow %q (Resource Group %q / Data Factory %q): %+v", dataflowName, id.ResourceGroup, dataFactoryName, err)
	}

	return nil
}

// Read Data Source blocks
func expandDataFactoryMappingDataFlowSources(input []interface{}) []datafactory.DataFlowSource {
	output := make([]datafactory.DataFlowSource, 0)
	for _, source := range input {
		output = append(output, expandDataFactoryMappingDataFlowSource(source))
	}
	return output
}
func expandDataFactoryMappingDataFlowSource(input interface{}) datafactory.DataFlowSource {
	fmt.Print(input)
	sourceBlock := input.(map[string]interface{})
	// prepare object
	datasetReferenceType := "DatasetReference"
	referenceName := sourceBlock["dataset_name"].(string)
	sourceName := sourceBlock["name"].(string)
	source := datafactory.DataFlowSource{
		Dataset: &datafactory.DatasetReference{
			Type:          &datasetReferenceType,
			ReferenceName: &referenceName,
		},
		Name: &sourceName,
	}
	return source
}
func flattenDataFactoryMappingDataFlowSources(input []datafactory.DataFlowSource) []interface{} {
	output := make([]interface{}, 0)
	for _, source := range input {
		output = append(output, expandDataFactoryMappingDataFlowSource(source))
	}
	return output
}
func flattenDataFactoryMappingDataFlowSource(input *datafactory.DataFlowSource) interface{} {
	output := make(map[string]interface{})
	output["dataset_name"] = input.Dataset.ReferenceName
	output["name"] = input.Name
	return output
}

// Read Data Sink blocks
func expandDataFactoryMappingDataFlowSinks(input []interface{}) []datafactory.DataFlowSink {
	output := make([]datafactory.DataFlowSink, 0)
	for _, sink := range input {
		output = append(output, expandDataFactoryMappingDataFlowSink(sink))
	}
	return output
}

func expandDataFactoryMappingDataFlowSink(input interface{}) datafactory.DataFlowSink {
	sinkBlock := input.(map[string]interface{})

	datasetReferenceType := "DatasetReference"
	referenceName := sinkBlock["dataset_name"].(string)
	sinkName := sinkBlock["name"].(string)

	sink := datafactory.DataFlowSink{
		Dataset: &datafactory.DatasetReference{
			Type:          &datasetReferenceType,
			ReferenceName: &referenceName,
		},
		Name: &sinkName,
	}
	return sink
}
func flattenDataFactoryMappingDataFlowSinks(input []datafactory.DataFlowSink) []interface{} {
	output := make([]interface{}, 0)
	for _, source := range input {
		output = append(output, expandDataFactoryMappingDataFlowSource(source))
	}
	return output
}
func flattenDataFactoryMappingDataFlowSink(input *datafactory.DataFlowSink) interface{} {
	output := make(map[string]interface{})
	output["dataset_name"] = input.Dataset.ReferenceName
	output["name"] = input.Name
	return output
}

// Read Data Transformation Blocks
func expandDataFactoryMappingDataFlowTransformations(input []interface{}) []datafactory.Transformation {
	output := make([]datafactory.Transformation, 0)
	for _, transformation := range input {
		output = append(output, expandDataFactoryMappingDataFlowTransformation(transformation))
	}
	return output
}

func expandDataFactoryMappingDataFlowTransformation(input interface{}) datafactory.Transformation {
	// prepare object
	transBlock := input.(map[string]interface{})
	transformationName := transBlock["name"].(string)
	transformation := datafactory.Transformation{
		Name: &transformationName,
	}
	return transformation
}
func flattenDataFactoryMappingDataFlowTransformations(input []datafactory.Transformation) []interface{} {
	output := make([]interface{}, 0)
	for _, source := range input {
		output = append(output, expandDataFactoryMappingDataFlowSource(source))
	}
	return output
}
func flattenDataFactoryMappingDataFlowTransformation(input *datafactory.Transformation) interface{} {
	output := make(map[string]interface{})
	output["name"] = input.Name
	return output
}
