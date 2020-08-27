package datafactory_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/acceptance"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func TestAccAzureRMDataFactoryMappingDataFlow_basic(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_data_factory_dataflow_mapping", "test")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { acceptance.PreCheck(t) },
		Providers:    acceptance.SupportedProviders,
		CheckDestroy: testCheckAzureRMDataFactoryMappingDataFlowDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAzureRMDataFactoryMappingDataFlow_basic(data),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMDataFactoryMappingDataFlowExists(data.ResourceName),
				),
			},
			data.ImportStep(),
		},
	})
}

func testCheckAzureRMDataFactoryMappingDataFlowExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := acceptance.AzureProvider.Meta().(*clients.Client).DataFactory.DatasetClient
		ctx := acceptance.AzureProvider.Meta().(*clients.Client).StopContext

		// Ensure we have enough information in state to look up in API
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}

		name := rs.Primary.Attributes["name"]
		resourceGroup, hasResourceGroup := rs.Primary.Attributes["resource_group_name"]
		dataFactoryName := rs.Primary.Attributes["data_factory_name"]
		if !hasResourceGroup {
			return fmt.Errorf("Bad: no resource group found in state for Data Factory: %s", name)
		}

		resp, err := client.Get(ctx, resourceGroup, dataFactoryName, name, "")
		if err != nil {
			return fmt.Errorf("Bad: Get on dataFactoryDatasetClient: %+v", err)
		}

		if utils.ResponseWasNotFound(resp.Response) {
			return fmt.Errorf("Bad: Data Factory Dataset HTTP %q (data factory name: %q / resource group: %q) does not exist", name, dataFactoryName, resourceGroup)
		}

		return nil
	}
}

func testCheckAzureRMDataFactoryMappingDataFlowDestroy(s *terraform.State) error {
	client := acceptance.AzureProvider.Meta().(*clients.Client).DataFactory.DatasetClient
	ctx := acceptance.AzureProvider.Meta().(*clients.Client).StopContext

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "azurerm_data_factory_dataflow_mapping" {
			continue
		}

		name := rs.Primary.Attributes["name"]
		resourceGroup := rs.Primary.Attributes["resource_group_name"]
		dataFactoryName := rs.Primary.Attributes["data_factory_name"]

		resp, err := client.Get(ctx, resourceGroup, dataFactoryName, name, "")

		if err != nil {
			return nil
		}

		if resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("Data Factory Dataset HTTP still exists:\n%#v", resp.Properties)
		}
	}

	return nil
}

func testAccAzureRMDataFactoryMappingDataFlow_basic(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-df-%d"
  location = "%s"
}

resource "azurerm_data_factory" "test" {
  name                = "acctestdf%d"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_data_factory_linked_service_web" "test" {
name                = "acctestlsweb%d"
resource_group_name = azurerm_resource_group.test.name
data_factory_name   = azurerm_data_factory.test.name
authentication_type = "Anonymous"
url                 = "http://www.bing.com"
}
  
resource "azurerm_data_factory_dataset_json" "test" {
name                = "acctestds%d"
resource_group_name = azurerm_resource_group.test.name
data_factory_name   = azurerm_data_factory.test.name
linked_service_name = azurerm_data_factory_linked_service_web.test.name

http_server_location {
	relative_url = "/fizz/buzz/"
	path         = "foo/bar/"
	filename     = "foo.json"
}

encoding = "UTF-8"
}

resource "azurerm_data_factory_dataset_json" "test2" {
name                = "acctestds%d"
resource_group_name = azurerm_resource_group.test.name
data_factory_name   = azurerm_data_factory.test.name
linked_service_name = azurerm_data_factory_linked_service_web.test.name

http_server_location {
	relative_url = "/fizz/buzz/"
	path         = "foo/bar/"
	filename     = "foo.json"
}

encoding = "UTF-8"
}

resource "azurerm_data_factory_dataflow_mapping" "test" {
  name                = "acctestdfm%d"
  resource_group_name = azurerm_resource_group.test.name
  data_factory_name   = azurerm_data_factory.test.name
  
  source {
	dataset_name = azurerm_data_factory_dataset_json.test.name
	name = "MemberData"
  }

  sink {
	dataset_name = azurerm_data_factory_dataset_json.test2.name
	name = "MemberDataSink"
  }

  transformation {
	name = "DeleteMembers"
  }

  script = "Foo"

}
`, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomInteger)
}
