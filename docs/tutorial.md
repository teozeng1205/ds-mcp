# Build an MCP server

> Get started building your own server to use in Claude for Desktop and other clients.

In this tutorial, we'll build a simple MCP weather server and connect it to a host, Claude for Desktop.

### What we'll be building

We'll build a server that exposes two tools: `get_alerts` and `get_forecast`. Then we'll connect the server to an MCP host (in this case, Claude for Desktop):

<Frame>
  <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=dce7b2f8a06c20ba358e4bd2e75fa4c7" data-og-width="2780" width="2780" data-og-height="1849" height="1849" data-path="images/current-weather.png" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=bbb19f34c5df59f66bc6bbb75d2bc5ed 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=2392d7e765b897c5b78f9f53d41439d4 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=dc349e75341b046d35a649762774da49 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=deeb99214d9383ee4a0c8aaacb120049 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=5c6f948059635e376deeadce3893e9b9 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=3922160478785cc88d5e98d418e8f7dd 2500w" />
</Frame>

<Note>
  Servers can connect to any client. We've chosen Claude for Desktop here for simplicity, but we also have guides on [building your own client](/docs/develop/build-client) as well as a [list of other clients here](/clients).
</Note>

### Core MCP Concepts

MCP servers can provide three main types of capabilities:

1. **[Resources](/docs/learn/server-concepts#resources)**: File-like data that can be read by clients (like API responses or file contents)
2. **[Tools](/docs/learn/server-concepts#tools)**: Functions that can be called by the LLM (with user approval)
3. **[Prompts](/docs/learn/server-concepts#prompts)**: Pre-written templates that help users accomplish specific tasks

This tutorial will primarily focus on tools.

<Tabs>
  <Tab title="Python">
    Let's get started with building our weather server! [You can find the complete code for what we'll be building here.](https://github.com/modelcontextprotocol/quickstart-resources/tree/main/weather-server-python)

    ### Prerequisite knowledge

    This quickstart assumes you have familiarity with:

    * Python
    * LLMs like Claude

    ### Logging in MCP Servers

    When implementing MCP servers, be careful about how you handle logging:

    **For STDIO-based servers:** Never write to standard output (stdout). This includes:

    * `print()` statements in Python
    * `console.log()` in JavaScript
    * `fmt.Println()` in Go
    * Similar stdout functions in other languages

    Writing to stdout will corrupt the JSON-RPC messages and break your server.

    **For HTTP-based servers:** Standard output logging is fine since it doesn't interfere with HTTP responses.

    ### Best Practices

    1. Use a logging library that writes to stderr or files.
    2. Tool names should follow the format specified [here](/specification/draft/server/tools#tool-names).

    ### Quick Examples

    ```python  theme={null}
    # ❌ Bad (STDIO)
    print("Processing request")

    # ✅ Good (STDIO)
    import logging
    logging.info("Processing request")
    ```

    ### System requirements

    * Python 3.10 or higher installed.
    * You must use the Python MCP SDK 1.2.0 or higher.

    ### Set up your environment

    First, let's install `uv` and set up our Python project and environment:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      curl -LsSf https://astral.sh/uv/install.sh | sh
      ```

      ```powershell Windows theme={null}
      powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
      ```
    </CodeGroup>

    Make sure to restart your terminal afterwards to ensure that the `uv` command gets picked up.

    Now, let's create and set up our project:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      # Create a new directory for our project
      uv init weather
      cd weather

      # Create virtual environment and activate it
      uv venv
      source .venv/bin/activate

      # Install dependencies
      uv add "mcp[cli]" httpx

      # Create our server file
      touch weather.py
      ```

      ```powershell Windows theme={null}
      # Create a new directory for our project
      uv init weather
      cd weather

      # Create virtual environment and activate it
      uv venv
      .venv\Scripts\activate

      # Install dependencies
      uv add mcp[cli] httpx

      # Create our server file
      new-item weather.py
      ```
    </CodeGroup>

    Now let's dive into building your server.

    ## Building your server

    ### Importing packages and setting up the instance

    Add these to the top of your `weather.py`:

    ```python  theme={null}
    from typing import Any
    import httpx
    from mcp.server.fastmcp import FastMCP

    # Initialize FastMCP server
    mcp = FastMCP("weather")

    # Constants
    NWS_API_BASE = "https://api.weather.gov"
    USER_AGENT = "weather-app/1.0"
    ```

    The FastMCP class uses Python type hints and docstrings to automatically generate tool definitions, making it easy to create and maintain MCP tools.

    ### Helper functions

    Next, let's add our helper functions for querying and formatting the data from the National Weather Service API:

    ```python  theme={null}
    async def make_nws_request(url: str) -> dict[str, Any] | None:
        """Make a request to the NWS API with proper error handling."""
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/geo+json"
        }
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, timeout=30.0)
                response.raise_for_status()
                return response.json()
            except Exception:
                return None

    def format_alert(feature: dict) -> str:
        """Format an alert feature into a readable string."""
        props = feature["properties"]
        return f"""
    Event: {props.get('event', 'Unknown')}
    Area: {props.get('areaDesc', 'Unknown')}
    Severity: {props.get('severity', 'Unknown')}
    Description: {props.get('description', 'No description available')}
    Instructions: {props.get('instruction', 'No specific instructions provided')}
    """
    ```

    ### Implementing tool execution

    The tool execution handler is responsible for actually executing the logic of each tool. Let's add it:

    ```python  theme={null}
    @mcp.tool()
    async def get_alerts(state: str) -> str:
        """Get weather alerts for a US state.

        Args:
            state: Two-letter US state code (e.g. CA, NY)
        """
        url = f"{NWS_API_BASE}/alerts/active/area/{state}"
        data = await make_nws_request(url)

        if not data or "features" not in data:
            return "Unable to fetch alerts or no alerts found."

        if not data["features"]:
            return "No active alerts for this state."

        alerts = [format_alert(feature) for feature in data["features"]]
        return "\n---\n".join(alerts)

    @mcp.tool()
    async def get_forecast(latitude: float, longitude: float) -> str:
        """Get weather forecast for a location.

        Args:
            latitude: Latitude of the location
            longitude: Longitude of the location
        """
        # First get the forecast grid endpoint
        points_url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
        points_data = await make_nws_request(points_url)

        if not points_data:
            return "Unable to fetch forecast data for this location."

        # Get the forecast URL from the points response
        forecast_url = points_data["properties"]["forecast"]
        forecast_data = await make_nws_request(forecast_url)

        if not forecast_data:
            return "Unable to fetch detailed forecast."

        # Format the periods into a readable forecast
        periods = forecast_data["properties"]["periods"]
        forecasts = []
        for period in periods[:5]:  # Only show next 5 periods
            forecast = f"""
    {period['name']}:
    Temperature: {period['temperature']}°{period['temperatureUnit']}
    Wind: {period['windSpeed']} {period['windDirection']}
    Forecast: {period['detailedForecast']}
    """
            forecasts.append(forecast)

    return "\n---\n".join(forecasts)
    ```

    ### Notes for Claude Desktop

    When connecting DS‑MCP servers to Claude Desktop, keep your tools minimal and predictable. In this repository, tools perform macro expansion and safety checks inside each tool function (e.g., `query_audit`, `query_anomalies`). This keeps behavior consistent regardless of host environment.

    ### Running the server

    Finally, let's initialize and run the server:

    ```python  theme={null}
    def main():
        # Initialize and run the server
        mcp.run(transport='stdio')

    if __name__ == "__main__":
        main()
    ```

    Your server is complete! Run `uv run weather.py` to start the MCP server, which will listen for messages from MCP hosts.

    Let's now test your server from an existing MCP host, Claude for Desktop.

    ## Testing your server with Claude for Desktop

    <Note>
      Claude for Desktop is not yet available on Linux. Linux users can proceed to the [Building a client](/docs/develop/build-client) tutorial to build an MCP client that connects to the server we just built.
    </Note>

    First, make sure you have Claude for Desktop installed. [You can install the latest version
    here.](https://claude.ai/download) If you already have Claude for Desktop, **make sure it's updated to the latest version.**

    We'll need to configure Claude for Desktop for whichever MCP servers you want to use. To do this, open your Claude for Desktop App configuration at `~/Library/Application Support/Claude/claude_desktop_config.json` in a text editor. Make sure to create the file if it doesn't exist.

    For example, if you have [VS Code](https://code.visualstudio.com/) installed:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      code ~/Library/Application\ Support/Claude/claude_desktop_config.json
      ```

      ```powershell Windows theme={null}
      code $env:AppData\Claude\claude_desktop_config.json
      ```
    </CodeGroup>

    You'll then add your servers in the `mcpServers` key. The MCP UI elements will only show up in Claude for Desktop if at least one server is properly configured.

    In this case, we'll add our single weather server like so:

    <CodeGroup>
      ```json macOS/Linux theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "uv",
            "args": [
              "--directory",
              "/ABSOLUTE/PATH/TO/PARENT/FOLDER/weather",
              "run",
              "weather.py"
            ]
          }
        }
      }
      ```

      ```json Windows theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "uv",
            "args": [
              "--directory",
              "C:\\ABSOLUTE\\PATH\\TO\\PARENT\\FOLDER\\weather",
              "run",
              "weather.py"
            ]
          }
        }
      }
      ```
    </CodeGroup>

    <Warning>
      You may need to put the full path to the `uv` executable in the `command` field. You can get this by running `which uv` on macOS/Linux or `where uv` on Windows.
    </Warning>

    <Note>
      Make sure you pass in the absolute path to your server. You can get this by running `pwd` on macOS/Linux or `cd` on Windows Command Prompt. On Windows, remember to use double backslashes (`\\`) or forward slashes (`/`) in the JSON path.
    </Note>

    This tells Claude for Desktop:

    1. There's an MCP server named "weather"
    2. To launch it by running `uv --directory /ABSOLUTE/PATH/TO/PARENT/FOLDER/weather run weather.py`

    Save the file, and restart **Claude for Desktop**.
  </Tab>

  <Tab title="Node">
    Let's get started with building our weather server! [You can find the complete code for what we'll be building here.](https://github.com/modelcontextprotocol/quickstart-resources/tree/main/weather-server-typescript)

    ### Prerequisite knowledge

    This quickstart assumes you have familiarity with:

    * TypeScript
    * LLMs like Claude

    ### Logging in MCP Servers

    When implementing MCP servers, be careful about how you handle logging:

    **For STDIO-based servers:** Never write to standard output (stdout). This includes:

    * `print()` statements in Python
    * `console.log()` in JavaScript
    * `fmt.Println()` in Go
    * Similar stdout functions in other languages

    Writing to stdout will corrupt the JSON-RPC messages and break your server.

    **For HTTP-based servers:** Standard output logging is fine since it doesn't interfere with HTTP responses.

    ### Best Practices

    1. Use a logging library that writes to stderr or files, such as `logging` in Python.
    2. For JavaScript, be especially careful - `console.log()` writes to stdout by default

    ### Quick Examples

    ```javascript  theme={null}
    // ❌ Bad (STDIO)
    console.log("Server started");

    // ✅ Good (STDIO)
    console.error("Server started"); // stderr is safe
    ```

    ### System requirements

    For TypeScript, make sure you have the latest version of Node installed.

    ### Set up your environment

    First, let's install Node.js and npm if you haven't already. You can download them from [nodejs.org](https://nodejs.org/).
    Verify your Node.js installation:

    ```bash  theme={null}
    node --version
    npm --version
    ```

    For this tutorial, you'll need Node.js version 16 or higher.

    Now, let's create and set up our project:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      # Create a new directory for our project
      mkdir weather
      cd weather

      # Initialize a new npm project
      npm init -y

      # Install dependencies
      npm install @modelcontextprotocol/sdk zod@3
      npm install -D @types/node typescript

      # Create our files
      mkdir src
      touch src/index.ts
      ```

      ```powershell Windows theme={null}
      # Create a new directory for our project
      md weather
      cd weather

      # Initialize a new npm project
      npm init -y

      # Install dependencies
      npm install @modelcontextprotocol/sdk zod@3
      npm install -D @types/node typescript

      # Create our files
      md src
      new-item src\index.ts
      ```
    </CodeGroup>

    Update your package.json to add type: "module" and a build script:

    ```json package.json theme={null}
    {
      "type": "module",
      "bin": {
        "weather": "./build/index.js"
      },
      "scripts": {
        "build": "tsc && chmod 755 build/index.js"
      },
      "files": ["build"]
    }
    ```

    Create a `tsconfig.json` in the root of your project:

    ```json tsconfig.json theme={null}
    {
      "compilerOptions": {
        "target": "ES2022",
        "module": "Node16",
        "moduleResolution": "Node16",
        "outDir": "./build",
        "rootDir": "./src",
        "strict": true,
        "esModuleInterop": true,
        "skipLibCheck": true,
        "forceConsistentCasingInFileNames": true
      },
      "include": ["src/**/*"],
      "exclude": ["node_modules"]
    }
    ```

    Now let's dive into building your server.

    ## Building your server

    ### Importing packages and setting up the instance

    Add these to the top of your `src/index.ts`:

    ```typescript  theme={null}
    import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
    import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
    import { z } from "zod";

    const NWS_API_BASE = "https://api.weather.gov";
    const USER_AGENT = "weather-app/1.0";

    // Create server instance
    const server = new McpServer({
      name: "weather",
      version: "1.0.0",
      capabilities: {
        resources: {},
        tools: {},
      },
    });
    ```

    ### Helper functions

    Next, let's add our helper functions for querying and formatting the data from the National Weather Service API:

    ```typescript  theme={null}
    // Helper function for making NWS API requests
    async function makeNWSRequest<T>(url: string): Promise<T | null> {
      const headers = {
        "User-Agent": USER_AGENT,
        Accept: "application/geo+json",
      };

      try {
        const response = await fetch(url, { headers });
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return (await response.json()) as T;
      } catch (error) {
        console.error("Error making NWS request:", error);
        return null;
      }
    }

    interface AlertFeature {
      properties: {
        event?: string;
        areaDesc?: string;
        severity?: string;
        status?: string;
        headline?: string;
      };
    }

    // Format alert data
    function formatAlert(feature: AlertFeature): string {
      const props = feature.properties;
      return [
        `Event: ${props.event || "Unknown"}`,
        `Area: ${props.areaDesc || "Unknown"}`,
        `Severity: ${props.severity || "Unknown"}`,
        `Status: ${props.status || "Unknown"}`,
        `Headline: ${props.headline || "No headline"}`,
        "---",
      ].join("\n");
    }

    interface ForecastPeriod {
      name?: string;
      temperature?: number;
      temperatureUnit?: string;
      windSpeed?: string;
      windDirection?: string;
      shortForecast?: string;
    }

    interface AlertsResponse {
      features: AlertFeature[];
    }

    interface PointsResponse {
      properties: {
        forecast?: string;
      };
    }

    interface ForecastResponse {
      properties: {
        periods: ForecastPeriod[];
      };
    }
    ```

    ### Implementing tool execution

    The tool execution handler is responsible for actually executing the logic of each tool. Let's add it:

    ```typescript  theme={null}
    // Register weather tools
    server.tool(
      "get_alerts",
      "Get weather alerts for a state",
      {
        state: z.string().length(2).describe("Two-letter state code (e.g. CA, NY)"),
      },
      async ({ state }) => {
        const stateCode = state.toUpperCase();
        const alertsUrl = `${NWS_API_BASE}/alerts?area=${stateCode}`;
        const alertsData = await makeNWSRequest<AlertsResponse>(alertsUrl);

        if (!alertsData) {
          return {
            content: [
              {
                type: "text",
                text: "Failed to retrieve alerts data",
              },
            ],
          };
        }

        const features = alertsData.features || [];
        if (features.length === 0) {
          return {
            content: [
              {
                type: "text",
                text: `No active alerts for ${stateCode}`,
              },
            ],
          };
        }

        const formattedAlerts = features.map(formatAlert);
        const alertsText = `Active alerts for ${stateCode}:\n\n${formattedAlerts.join("\n")}`;

        return {
          content: [
            {
              type: "text",
              text: alertsText,
            },
          ],
        };
      },
    );

    server.tool(
      "get_forecast",
      "Get weather forecast for a location",
      {
        latitude: z.number().min(-90).max(90).describe("Latitude of the location"),
        longitude: z
          .number()
          .min(-180)
          .max(180)
          .describe("Longitude of the location"),
      },
      async ({ latitude, longitude }) => {
        // Get grid point data
        const pointsUrl = `${NWS_API_BASE}/points/${latitude.toFixed(4)},${longitude.toFixed(4)}`;
        const pointsData = await makeNWSRequest<PointsResponse>(pointsUrl);

        if (!pointsData) {
          return {
            content: [
              {
                type: "text",
                text: `Failed to retrieve grid point data for coordinates: ${latitude}, ${longitude}. This location may not be supported by the NWS API (only US locations are supported).`,
              },
            ],
          };
        }

        const forecastUrl = pointsData.properties?.forecast;
        if (!forecastUrl) {
          return {
            content: [
              {
                type: "text",
                text: "Failed to get forecast URL from grid point data",
              },
            ],
          };
        }

        // Get forecast data
        const forecastData = await makeNWSRequest<ForecastResponse>(forecastUrl);
        if (!forecastData) {
          return {
            content: [
              {
                type: "text",
                text: "Failed to retrieve forecast data",
              },
            ],
          };
        }

        const periods = forecastData.properties?.periods || [];
        if (periods.length === 0) {
          return {
            content: [
              {
                type: "text",
                text: "No forecast periods available",
              },
            ],
          };
        }

        // Format forecast periods
        const formattedForecast = periods.map((period: ForecastPeriod) =>
          [
            `${period.name || "Unknown"}:`,
            `Temperature: ${period.temperature || "Unknown"}°${period.temperatureUnit || "F"}`,
            `Wind: ${period.windSpeed || "Unknown"} ${period.windDirection || ""}`,
            `${period.shortForecast || "No forecast available"}`,
            "---",
          ].join("\n"),
        );

        const forecastText = `Forecast for ${latitude}, ${longitude}:\n\n${formattedForecast.join("\n")}`;

        return {
          content: [
            {
              type: "text",
              text: forecastText,
            },
          ],
        };
      },
    );
    ```

    ### Running the server

    Finally, implement the main function to run the server:

    ```typescript  theme={null}
    async function main() {
      const transport = new StdioServerTransport();
      await server.connect(transport);
      console.error("Weather MCP Server running on stdio");
    }

    main().catch((error) => {
      console.error("Fatal error in main():", error);
      process.exit(1);
    });
    ```

    Make sure to run `npm run build` to build your server! This is a very important step in getting your server to connect.

    Let's now test your server from an existing MCP host, Claude for Desktop.

    ## Testing your server with Claude for Desktop

    <Note>
      Claude for Desktop is not yet available on Linux. Linux users can proceed to the [Building a client](/docs/develop/build-client) tutorial to build an MCP client that connects to the server we just built.
    </Note>

    First, make sure you have Claude for Desktop installed. [You can install the latest version
    here.](https://claude.ai/download) If you already have Claude for Desktop, **make sure it's updated to the latest version.**

    We'll need to configure Claude for Desktop for whichever MCP servers you want to use. To do this, open your Claude for Desktop App configuration at `~/Library/Application Support/Claude/claude_desktop_config.json` in a text editor. Make sure to create the file if it doesn't exist.

    For example, if you have [VS Code](https://code.visualstudio.com/) installed:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      code ~/Library/Application\ Support/Claude/claude_desktop_config.json
      ```

      ```powershell Windows theme={null}
      code $env:AppData\Claude\claude_desktop_config.json
      ```
    </CodeGroup>

    You'll then add your servers in the `mcpServers` key. The MCP UI elements will only show up in Claude for Desktop if at least one server is properly configured.

    In this case, we'll add our single weather server like so:

    <CodeGroup>
      ```json macOS/Linux theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "node",
            "args": ["/ABSOLUTE/PATH/TO/PARENT/FOLDER/weather/build/index.js"]
          }
        }
      }
      ```

      ```json Windows theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "node",
            "args": ["C:\\PATH\\TO\\PARENT\\FOLDER\\weather\\build\\index.js"]
          }
        }
      }
      ```
    </CodeGroup>

    This tells Claude for Desktop:

    1. There's an MCP server named "weather"
    2. Launch it by running `node /ABSOLUTE/PATH/TO/PARENT/FOLDER/weather/build/index.js`

    Save the file, and restart **Claude for Desktop**.
  </Tab>

  <Tab title="Java">
    <Note>
      This is a quickstart demo based on Spring AI MCP auto-configuration and boot starters.
      To learn how to create sync and async MCP Servers, manually, consult the [Java SDK Server](/sdk/java/mcp-server) documentation.
    </Note>

    Let's get started with building our weather server!
    [You can find the complete code for what we'll be building here.](https://github.com/spring-projects/spring-ai-examples/tree/main/model-context-protocol/weather/starter-stdio-server)

    For more information, see the [MCP Server Boot Starter](https://docs.spring.io/spring-ai/reference/api/mcp/mcp-server-boot-starter-docs.html) reference documentation.
    For manual MCP Server implementation, refer to the [MCP Server Java SDK documentation](/sdk/java/mcp-server).

    ### Logging in MCP Servers

    When implementing MCP servers, be careful about how you handle logging:

    **For STDIO-based servers:** Never write to standard output (stdout). This includes:

    * `print()` statements in Python
    * `console.log()` in JavaScript
    * `fmt.Println()` in Go
    * Similar stdout functions in other languages

    Writing to stdout will corrupt the JSON-RPC messages and break your server.

    **For HTTP-based servers:** Standard output logging is fine since it doesn't interfere with HTTP responses.

    ### Best Practices

    1. Use a logging library that writes to stderr or files.
    2. Ensure any configured logging library will not write to STDOUT

    ### System requirements

    * Java 17 or higher installed.
    * [Spring Boot 3.3.x](https://docs.spring.io/spring-boot/installing.html) or higher

    ### Set up your environment

    Use the [Spring Initializer](https://start.spring.io/) to bootstrap the project.

    You will need to add the following dependencies:

    <CodeGroup>
      ```xml Maven theme={null}
      <dependencies>
            <dependency>
                <groupId>org.springframework.ai</groupId>
                <artifactId>spring-ai-starter-mcp-server</artifactId>
            </dependency>

            <dependency>
                <groupId>org.springframework</groupId>
                <artifactId>spring-web</artifactId>
            </dependency>
      </dependencies>
      ```

      ```groovy Gradle theme={null}
      dependencies {
        implementation platform("org.springframework.ai:spring-ai-starter-mcp-server")
        implementation platform("org.springframework:spring-web")
      }
      ```
    </CodeGroup>

    Then configure your application by setting the application properties:

    <CodeGroup>
      ```bash application.properties theme={null}
      spring.main.bannerMode=off
      logging.pattern.console=
      ```

      ```yaml application.yml theme={null}
      logging:
        pattern:
          console:
      spring:
        main:
          banner-mode: off
      ```
    </CodeGroup>

    The [Server Configuration Properties](https://docs.spring.io/spring-ai/reference/api/mcp/mcp-server-boot-starter-docs.html#_configuration_properties) documents all available properties.

    Now let's dive into building your server.

    ## Building your server

    ### Weather Service

    Let's implement a [WeatherService.java](https://github.com/spring-projects/spring-ai-examples/blob/main/model-context-protocol/weather/starter-stdio-server/src/main/java/org/springframework/ai/mcp/sample/server/WeatherService.java) that uses a REST client to query the data from the National Weather Service API:

    ```java  theme={null}
    @Service
    public class WeatherService {

    	private final RestClient restClient;

    	public WeatherService() {
    		this.restClient = RestClient.builder()
    			.baseUrl("https://api.weather.gov")
    			.defaultHeader("Accept", "application/geo+json")
    			.defaultHeader("User-Agent", "WeatherApiClient/1.0 (your@email.com)")
    			.build();
    	}

      @Tool(description = "Get weather forecast for a specific latitude/longitude")
      public String getWeatherForecastByLocation(
          double latitude,   // Latitude coordinate
          double longitude   // Longitude coordinate
      ) {
          // Returns detailed forecast including:
          // - Temperature and unit
          // - Wind speed and direction
          // - Detailed forecast description
      }

      @Tool(description = "Get weather alerts for a US state")
      public String getAlerts(
          @ToolParam(description = "Two-letter US state code (e.g. CA, NY)") String state
      ) {
          // Returns active alerts including:
          // - Event type
          // - Affected area
          // - Severity
          // - Description
          // - Safety instructions
      }

      // ......
    }
    ```

    The `@Service` annotation with auto-register the service in your application context.
    The Spring AI `@Tool` annotation, making it easy to create and maintain MCP tools.

    The auto-configuration will automatically register these tools with the MCP server.

    ### Create your Boot Application

    ```java  theme={null}
    @SpringBootApplication
    public class McpServerApplication {

    	public static void main(String[] args) {
    		SpringApplication.run(McpServerApplication.class, args);
    	}

    	@Bean
    	public ToolCallbackProvider weatherTools(WeatherService weatherService) {
    		return  MethodToolCallbackProvider.builder().toolObjects(weatherService).build();
    	}
    }
    ```

    Uses the `MethodToolCallbackProvider` utils to convert the `@Tools` into actionable callbacks used by the MCP server.

    ### Running the server

    Finally, let's build the server:

    ```bash  theme={null}
    ./mvnw clean install
    ```

    This will generate a `mcp-weather-stdio-server-0.0.1-SNAPSHOT.jar` file within the `target` folder.

    Let's now test your server from an existing MCP host, Claude for Desktop.

    ## Testing your server with Claude for Desktop

    <Note>
      Claude for Desktop is not yet available on Linux.
    </Note>

    First, make sure you have Claude for Desktop installed.
    [You can install the latest version here.](https://claude.ai/download) If you already have Claude for Desktop, **make sure it's updated to the latest version.**

    We'll need to configure Claude for Desktop for whichever MCP servers you want to use.
    To do this, open your Claude for Desktop App configuration at `~/Library/Application Support/Claude/claude_desktop_config.json` in a text editor.
    Make sure to create the file if it doesn't exist.

    For example, if you have [VS Code](https://code.visualstudio.com/) installed:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      code ~/Library/Application\ Support/Claude/claude_desktop_config.json
      ```

      ```powershell Windows theme={null}
      code $env:AppData\Claude\claude_desktop_config.json
      ```
    </CodeGroup>

    You'll then add your servers in the `mcpServers` key.
    The MCP UI elements will only show up in Claude for Desktop if at least one server is properly configured.

    In this case, we'll add our single weather server like so:

    <CodeGroup>
      ```json macOS/Linux theme={null}
      {
        "mcpServers": {
          "spring-ai-mcp-weather": {
            "command": "java",
            "args": [
              "-Dspring.ai.mcp.server.stdio=true",
              "-jar",
              "/ABSOLUTE/PATH/TO/PARENT/FOLDER/mcp-weather-stdio-server-0.0.1-SNAPSHOT.jar"
            ]
          }
        }
      }
      ```

      ```json Windows theme={null}
      {
        "mcpServers": {
          "spring-ai-mcp-weather": {
            "command": "java",
            "args": [
              "-Dspring.ai.mcp.server.transport=STDIO",
              "-jar",
              "C:\\ABSOLUTE\\PATH\\TO\\PARENT\\FOLDER\\weather\\mcp-weather-stdio-server-0.0.1-SNAPSHOT.jar"
            ]
          }
        }
      }
      ```
    </CodeGroup>

    <Note>
      Make sure you pass in the absolute path to your server.
    </Note>

    This tells Claude for Desktop:

    1. There's an MCP server named "my-weather-server"
    2. To launch it by running `java -jar /ABSOLUTE/PATH/TO/PARENT/FOLDER/mcp-weather-stdio-server-0.0.1-SNAPSHOT.jar`

    Save the file, and restart **Claude for Desktop**.

    ## Testing your server with Java client

    ### Create an MCP Client manually

    Use the `McpClient` to connect to the server:

    ```java  theme={null}
    var stdioParams = ServerParameters.builder("java")
      .args("-jar", "/ABSOLUTE/PATH/TO/PARENT/FOLDER/mcp-weather-stdio-server-0.0.1-SNAPSHOT.jar")
      .build();

    var stdioTransport = new StdioClientTransport(stdioParams);

    var mcpClient = McpClient.sync(stdioTransport).build();

    mcpClient.initialize();

    ListToolsResult toolsList = mcpClient.listTools();

    CallToolResult weather = mcpClient.callTool(
      new CallToolRequest("getWeatherForecastByLocation",
          Map.of("latitude", "47.6062", "longitude", "-122.3321")));

    CallToolResult alert = mcpClient.callTool(
      new CallToolRequest("getAlerts", Map.of("state", "NY")));

    mcpClient.closeGracefully();
    ```

    ### Use MCP Client Boot Starter

    Create a new boot starter application using the `spring-ai-starter-mcp-client` dependency:

    ```xml  theme={null}
    <dependency>
        <groupId>org.springframework.ai</groupId>
        <artifactId>spring-ai-starter-mcp-client</artifactId>
    </dependency>
    ```

    and set the `spring.ai.mcp.client.stdio.servers-configuration` property to point to your `claude_desktop_config.json`.
    You can reuse the existing Anthropic Desktop configuration:

    ```properties  theme={null}
    spring.ai.mcp.client.stdio.servers-configuration=file:PATH/TO/claude_desktop_config.json
    ```

    When you start your client application, the auto-configuration will create, automatically MCP clients from the claude\_desktop\_config.json.

    For more information, see the [MCP Client Boot Starters](https://docs.spring.io/spring-ai/reference/api/mcp/mcp-server-boot-client-docs.html) reference documentation.

    ## More Java MCP Server examples

    The [starter-webflux-server](https://github.com/spring-projects/spring-ai-examples/tree/main/model-context-protocol/weather/starter-webflux-server) demonstrates how to create an MCP server using SSE transport.
    It showcases how to define and register MCP Tools, Resources, and Prompts, using the Spring Boot's auto-configuration capabilities.
  </Tab>

  <Tab title="Kotlin">
    Let's get started with building our weather server! [You can find the complete code for what we'll be building here.](https://github.com/modelcontextprotocol/kotlin-sdk/tree/main/samples/weather-stdio-server)

    ### Prerequisite knowledge

    This quickstart assumes you have familiarity with:

    * Kotlin
    * LLMs like Claude

    ### System requirements

    * Java 17 or higher installed.

    ### Set up your environment

    First, let's install `java` and `gradle` if you haven't already.
    You can download `java` from [official Oracle JDK website](https://www.oracle.com/java/technologies/downloads/).
    Verify your `java` installation:

    ```bash  theme={null}
    java --version
    ```

    Now, let's create and set up your project:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      # Create a new directory for our project
      mkdir weather
      cd weather

      # Initialize a new kotlin project
      gradle init
      ```

      ```powershell Windows theme={null}
      # Create a new directory for our project
      md weather
      cd weather

      # Initialize a new kotlin project
      gradle init
      ```
    </CodeGroup>

    After running `gradle init`, you will be presented with options for creating your project.
    Select **Application** as the project type, **Kotlin** as the programming language, and **Java 17** as the Java version.

    Alternatively, you can create a Kotlin application using the [IntelliJ IDEA project wizard](https://kotlinlang.org/docs/jvm-get-started.html).

    After creating the project, add the following dependencies:

    <CodeGroup>
      ```kotlin build.gradle.kts theme={null}
      val mcpVersion = "0.4.0"
      val slf4jVersion = "2.0.9"
      val ktorVersion = "3.1.1"

      dependencies {
          implementation("io.modelcontextprotocol:kotlin-sdk:$mcpVersion")
          implementation("org.slf4j:slf4j-nop:$slf4jVersion")
          implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
          implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
      }
      ```

      ```groovy build.gradle theme={null}
      def mcpVersion = '0.3.0'
      def slf4jVersion = '2.0.9'
      def ktorVersion = '3.1.1'

      dependencies {
          implementation "io.modelcontextprotocol:kotlin-sdk:$mcpVersion"
          implementation "org.slf4j:slf4j-nop:$slf4jVersion"
          implementation "io.ktor:ktor-client-content-negotiation:$ktorVersion"
          implementation "io.ktor:ktor-serialization-kotlinx-json:$ktorVersion"
      }
      ```
    </CodeGroup>

    Also, add the following plugins to your build script:

    <CodeGroup>
      ```kotlin build.gradle.kts theme={null}
      plugins {
          kotlin("plugin.serialization") version "your_version_of_kotlin"
          id("com.github.johnrengelman.shadow") version "8.1.1"
      }
      ```

      ```groovy build.gradle theme={null}
      plugins {
          id 'org.jetbrains.kotlin.plugin.serialization' version 'your_version_of_kotlin'
          id 'com.github.johnrengelman.shadow' version '8.1.1'
      }
      ```
    </CodeGroup>

    Now let’s dive into building your server.

    ## Building your server

    ### Setting up the instance

    Add a server initialization function:

    ```kotlin  theme={null}
    // Main function to run the MCP server
    fun `run mcp server`() {
        // Create the MCP Server instance with a basic implementation
        val server = Server(
            Implementation(
                name = "weather", // Tool name is "weather"
                version = "1.0.0" // Version of the implementation
            ),
            ServerOptions(
                capabilities = ServerCapabilities(tools = ServerCapabilities.Tools(listChanged = true))
            )
        )

        // Create a transport using standard IO for server communication
        val transport = StdioServerTransport(
            System.`in`.asInput(),
            System.out.asSink().buffered()
        )

        runBlocking {
            server.connect(transport)
            val done = Job()
            server.onClose {
                done.complete()
            }
            done.join()
        }
    }
    ```

    ### Weather API helper functions

    Next, let's add functions and data classes for querying and converting responses from the National Weather Service API:

    ```kotlin  theme={null}
    // Extension function to fetch forecast information for given latitude and longitude
    suspend fun HttpClient.getForecast(latitude: Double, longitude: Double): List<String> {
        val points = this.get("/points/$latitude,$longitude").body<Points>()
        val forecast = this.get(points.properties.forecast).body<Forecast>()
        return forecast.properties.periods.map { period ->
            """
                ${period.name}:
                Temperature: ${period.temperature} ${period.temperatureUnit}
                Wind: ${period.windSpeed} ${period.windDirection}
                Forecast: ${period.detailedForecast}
            """.trimIndent()
        }
    }

    // Extension function to fetch weather alerts for a given state
    suspend fun HttpClient.getAlerts(state: String): List<String> {
        val alerts = this.get("/alerts/active/area/$state").body<Alert>()
        return alerts.features.map { feature ->
            """
                Event: ${feature.properties.event}
                Area: ${feature.properties.areaDesc}
                Severity: ${feature.properties.severity}
                Description: ${feature.properties.description}
                Instruction: ${feature.properties.instruction}
            """.trimIndent()
        }
    }

    @Serializable
    data class Points(
        val properties: Properties
    ) {
        @Serializable
        data class Properties(val forecast: String)
    }

    @Serializable
    data class Forecast(
        val properties: Properties
    ) {
        @Serializable
        data class Properties(val periods: List<Period>)

        @Serializable
        data class Period(
            val number: Int, val name: String, val startTime: String, val endTime: String,
            val isDaytime: Boolean, val temperature: Int, val temperatureUnit: String,
            val temperatureTrend: String, val probabilityOfPrecipitation: JsonObject,
            val windSpeed: String, val windDirection: String,
            val shortForecast: String, val detailedForecast: String,
        )
    }

    @Serializable
    data class Alert(
        val features: List<Feature>
    ) {
        @Serializable
        data class Feature(
            val properties: Properties
        )

        @Serializable
        data class Properties(
            val event: String, val areaDesc: String, val severity: String,
            val description: String, val instruction: String?,
        )
    }
    ```

    ### Implementing tool execution

    The tool execution handler is responsible for actually executing the logic of each tool. Let's add it:

    ```kotlin  theme={null}
    // Create an HTTP client with a default request configuration and JSON content negotiation
    val httpClient = HttpClient {
        defaultRequest {
            url("https://api.weather.gov")
            headers {
                append("Accept", "application/geo+json")
                append("User-Agent", "WeatherApiClient/1.0")
            }
            contentType(ContentType.Application.Json)
        }
        // Install content negotiation plugin for JSON serialization/deserialization
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
    }

    // Register a tool to fetch weather alerts by state
    server.addTool(
        name = "get_alerts",
        description = """
            Get weather alerts for a US state. Input is Two-letter US state code (e.g. CA, NY)
        """.trimIndent(),
        inputSchema = Tool.Input(
            properties = buildJsonObject {
                putJsonObject("state") {
                    put("type", "string")
                    put("description", "Two-letter US state code (e.g. CA, NY)")
                }
            },
            required = listOf("state")
        )
    ) { request ->
        val state = request.arguments["state"]?.jsonPrimitive?.content
        if (state == null) {
            return@addTool CallToolResult(
                content = listOf(TextContent("The 'state' parameter is required."))
            )
        }

        val alerts = httpClient.getAlerts(state)

        CallToolResult(content = alerts.map { TextContent(it) })
    }

    // Register a tool to fetch weather forecast by latitude and longitude
    server.addTool(
        name = "get_forecast",
        description = """
            Get weather forecast for a specific latitude/longitude
        """.trimIndent(),
        inputSchema = Tool.Input(
            properties = buildJsonObject {
                putJsonObject("latitude") { put("type", "number") }
                putJsonObject("longitude") { put("type", "number") }
            },
            required = listOf("latitude", "longitude")
        )
    ) { request ->
        val latitude = request.arguments["latitude"]?.jsonPrimitive?.doubleOrNull
        val longitude = request.arguments["longitude"]?.jsonPrimitive?.doubleOrNull
        if (latitude == null || longitude == null) {
            return@addTool CallToolResult(
                content = listOf(TextContent("The 'latitude' and 'longitude' parameters are required."))
            )
        }

        val forecast = httpClient.getForecast(latitude, longitude)

        CallToolResult(content = forecast.map { TextContent(it) })
    }
    ```

    ### Running the server

    Finally, implement the main function to run the server:

    ```kotlin  theme={null}
    fun main() = `run mcp server`()
    ```

    Make sure to run `./gradlew build` to build your server. This is a very important step in getting your server to connect.

    Let's now test your server from an existing MCP host, Claude for Desktop.

    ## Testing your server with Claude for Desktop

    <Note>
      Claude for Desktop is not yet available on Linux. Linux users can proceed to the [Building a client](/docs/develop/build-client) tutorial to build an MCP client that connects to the server we just built.
    </Note>

    First, make sure you have Claude for Desktop installed. [You can install the latest version
    here.](https://claude.ai/download) If you already have Claude for Desktop, **make sure it's updated to the latest version.**

    We'll need to configure Claude for Desktop for whichever MCP servers you want to use.
    To do this, open your Claude for Desktop App configuration at `~/Library/Application Support/Claude/claude_desktop_config.json` in a text editor.
    Make sure to create the file if it doesn't exist.

    For example, if you have [VS Code](https://code.visualstudio.com/) installed:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      code ~/Library/Application\ Support/Claude/claude_desktop_config.json
      ```

      ```powershell Windows theme={null}
      code $env:AppData\Claude\claude_desktop_config.json
      ```
    </CodeGroup>

    You'll then add your servers in the `mcpServers` key.
    The MCP UI elements will only show up in Claude for Desktop if at least one server is properly configured.

    In this case, we'll add our single weather server like so:

    <CodeGroup>
      ```json macOS/Linux theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "java",
            "args": [
              "-jar",
              "/ABSOLUTE/PATH/TO/PARENT/FOLDER/weather/build/libs/weather-0.1.0-all.jar"
            ]
          }
        }
      }
      ```

      ```json Windows theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "java",
            "args": [
              "-jar",
              "C:\\PATH\\TO\\PARENT\\FOLDER\\weather\\build\\libs\\weather-0.1.0-all.jar"
            ]
          }
        }
      }
      ```
    </CodeGroup>

    This tells Claude for Desktop:

    1. There's an MCP server named "weather"
    2. Launch it by running `java -jar /ABSOLUTE/PATH/TO/PARENT/FOLDER/weather/build/libs/weather-0.1.0-all.jar`

    Save the file, and restart **Claude for Desktop**.
  </Tab>

  <Tab title="C#">
    Let's get started with building our weather server! [You can find the complete code for what we'll be building here.](https://github.com/modelcontextprotocol/csharp-sdk/tree/main/samples/QuickstartWeatherServer)

    ### Prerequisite knowledge

    This quickstart assumes you have familiarity with:

    * C#
    * LLMs like Claude
    * .NET 8 or higher

    ### Logging in MCP Servers

    When implementing MCP servers, be careful about how you handle logging:

    **For STDIO-based servers:** Never write to standard output (stdout). This includes:

    * `print()` statements in Python
    * `console.log()` in JavaScript
    * `fmt.Println()` in Go
    * Similar stdout functions in other languages

    Writing to stdout will corrupt the JSON-RPC messages and break your server.

    **For HTTP-based servers:** Standard output logging is fine since it doesn't interfere with HTTP responses.

    ### Best Practices

    1. Use a logging library that writes to stderr or files

    ### System requirements

    * [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) or higher installed.

    ### Set up your environment

    First, let's install `dotnet` if you haven't already. You can download `dotnet` from [official Microsoft .NET website](https://dotnet.microsoft.com/download/). Verify your `dotnet` installation:

    ```bash  theme={null}
    dotnet --version
    ```

    Now, let's create and set up your project:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      # Create a new directory for our project
      mkdir weather
      cd weather
      # Initialize a new C# project
      dotnet new console
      ```

      ```powershell Windows theme={null}
      # Create a new directory for our project
      mkdir weather
      cd weather
      # Initialize a new C# project
      dotnet new console
      ```
    </CodeGroup>

    After running `dotnet new console`, you will be presented with a new C# project.
    You can open the project in your favorite IDE, such as [Visual Studio](https://visualstudio.microsoft.com/) or [Rider](https://www.jetbrains.com/rider/).
    Alternatively, you can create a C# application using the [Visual Studio project wizard](https://learn.microsoft.com/en-us/visualstudio/get-started/csharp/tutorial-console?view=vs-2022).
    After creating the project, add NuGet package for the Model Context Protocol SDK and hosting:

    ```bash  theme={null}
    # Add the Model Context Protocol SDK NuGet package
    dotnet add package ModelContextProtocol --prerelease
    # Add the .NET Hosting NuGet package
    dotnet add package Microsoft.Extensions.Hosting
    ```

    Now let’s dive into building your server.

    ## Building your server

    Open the `Program.cs` file in your project and replace its contents with the following code:

    ```csharp  theme={null}
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using ModelContextProtocol;
    using System.Net.Http.Headers;

    var builder = Host.CreateEmptyApplicationBuilder(settings: null);

    builder.Services.AddMcpServer()
        .WithStdioServerTransport()
        .WithToolsFromAssembly();

    builder.Services.AddSingleton(_ =>
    {
        var client = new HttpClient() { BaseAddress = new Uri("https://api.weather.gov") };
        client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("weather-tool", "1.0"));
        return client;
    });

    var app = builder.Build();

    await app.RunAsync();
    ```

    <Note>
      When creating the `ApplicationHostBuilder`, ensure you use `CreateEmptyApplicationBuilder` instead of `CreateDefaultBuilder`. This ensures that the server does not write any additional messages to the console. This is only necessary for servers using STDIO transport.
    </Note>

    This code sets up a basic console application that uses the Model Context Protocol SDK to create an MCP server with standard I/O transport.

    ### Weather API helper functions

    Create an extension class for `HttpClient` which helps simplify JSON request handling:

    ```csharp  theme={null}
    using System.Text.Json;

    internal static class HttpClientExt
    {
        public static async Task<JsonDocument> ReadJsonDocumentAsync(this HttpClient client, string requestUri)
        {
            using var response = await client.GetAsync(requestUri);
            response.EnsureSuccessStatusCode();
            return await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
        }
    }
    ```

    Next, define a class with the tool execution handlers for querying and converting responses from the National Weather Service API:

    ```csharp  theme={null}
    using ModelContextProtocol.Server;
    using System.ComponentModel;
    using System.Globalization;
    using System.Text.Json;

    namespace QuickstartWeatherServer.Tools;

    [McpServerToolType]
    public static class WeatherTools
    {
        [McpServerTool, Description("Get weather alerts for a US state.")]
        public static async Task<string> GetAlerts(
            HttpClient client,
            [Description("The US state to get alerts for.")] string state)
        {
            using var jsonDocument = await client.ReadJsonDocumentAsync($"/alerts/active/area/{state}");
            var jsonElement = jsonDocument.RootElement;
            var alerts = jsonElement.GetProperty("features").EnumerateArray();

            if (!alerts.Any())
            {
                return "No active alerts for this state.";
            }

            return string.Join("\n--\n", alerts.Select(alert =>
            {
                JsonElement properties = alert.GetProperty("properties");
                return $"""
                        Event: {properties.GetProperty("event").GetString()}
                        Area: {properties.GetProperty("areaDesc").GetString()}
                        Severity: {properties.GetProperty("severity").GetString()}
                        Description: {properties.GetProperty("description").GetString()}
                        Instruction: {properties.GetProperty("instruction").GetString()}
                        """;
            }));
        }

        [McpServerTool, Description("Get weather forecast for a location.")]
        public static async Task<string> GetForecast(
            HttpClient client,
            [Description("Latitude of the location.")] double latitude,
            [Description("Longitude of the location.")] double longitude)
        {
            var pointUrl = string.Create(CultureInfo.InvariantCulture, $"/points/{latitude},{longitude}");
            using var jsonDocument = await client.ReadJsonDocumentAsync(pointUrl);
            var forecastUrl = jsonDocument.RootElement.GetProperty("properties").GetProperty("forecast").GetString()
                ?? throw new Exception($"No forecast URL provided by {client.BaseAddress}points/{latitude},{longitude}");

            using var forecastDocument = await client.ReadJsonDocumentAsync(forecastUrl);
            var periods = forecastDocument.RootElement.GetProperty("properties").GetProperty("periods").EnumerateArray();

            return string.Join("\n---\n", periods.Select(period => $"""
                    {period.GetProperty("name").GetString()}
                    Temperature: {period.GetProperty("temperature").GetInt32()}°F
                    Wind: {period.GetProperty("windSpeed").GetString()} {period.GetProperty("windDirection").GetString()}
                    Forecast: {period.GetProperty("detailedForecast").GetString()}
                    """));
        }
    }
    ```

    ### Running the server

    Finally, run the server using the following command:

    ```bash  theme={null}
    dotnet run
    ```

    This will start the server and listen for incoming requests on standard input/output.

    ## Testing your server with Claude for Desktop

    <Note>
      Claude for Desktop is not yet available on Linux. Linux users can proceed to the [Building a client](/docs/develop/build-client) tutorial to build an MCP client that connects to the server we just built.
    </Note>

    First, make sure you have Claude for Desktop installed. [You can install the latest version
    here.](https://claude.ai/download) If you already have Claude for Desktop, **make sure it's updated to the latest version.**
    We'll need to configure Claude for Desktop for whichever MCP servers you want to use. To do this, open your Claude for Desktop App configuration at `~/Library/Application Support/Claude/claude_desktop_config.json` in a text editor. Make sure to create the file if it doesn't exist.
    For example, if you have [VS Code](https://code.visualstudio.com/) installed:

    <CodeGroup>
      ```bash macOS/Linux theme={null}
      code ~/Library/Application\ Support/Claude/claude_desktop_config.json
      ```

      ```powershell Windows theme={null}
      code $env:AppData\Claude\claude_desktop_config.json
      ```
    </CodeGroup>

    You'll then add your servers in the `mcpServers` key. The MCP UI elements will only show up in Claude for Desktop if at least one server is properly configured.
    In this case, we'll add our single weather server like so:

    <CodeGroup>
      ```json macOS/Linux theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "dotnet",
            "args": ["run", "--project", "/ABSOLUTE/PATH/TO/PROJECT", "--no-build"]
          }
        }
      }
      ```

      ```json Windows theme={null}
      {
        "mcpServers": {
          "weather": {
            "command": "dotnet",
            "args": [
              "run",
              "--project",
              "C:\\ABSOLUTE\\PATH\\TO\\PROJECT",
              "--no-build"
            ]
          }
        }
      }
      ```
    </CodeGroup>

    This tells Claude for Desktop:

    1. There's an MCP server named "weather"
    2. Launch it by running `dotnet run /ABSOLUTE/PATH/TO/PROJECT`
       Save the file, and restart **Claude for Desktop**.
  </Tab>
</Tabs>

### Test with commands

Let's make sure Claude for Desktop is picking up the two tools we've exposed in our `weather` server. You can do this by looking for the "Search and tools" <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=2742ec3fb97067e8591e68546c90221e" style={{display: 'inline', margin: 0, height: '1.3em'}} data-og-width="24" width="24" data-og-height="24" height="24" data-path="images/claude-desktop-mcp-slider.svg" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=52839f8519f476623c4fb5bb87ee24bd 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=f0491976e108286441fc6554309c5c4f 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=08e83eb102eda755a7db1eb27d16ebff 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=2524a80752928b0206e68e8e1890d1aa 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=3c0dc88dadad5ed8e8af316965d00e0b 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=702363a955a631c40c342f9557d5cfdd 2500w" /> icon:

<Frame>
  <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=dee15f1044586f26f7c8f489d1b1bea1" data-og-width="2250" width="2250" data-og-height="462" height="462" data-path="images/visual-indicator-mcp-tools.png" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=7d5a49e662150224cacc011f27310bd0 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=8f4d36114ab38529374b8ed651bec92d 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=6e0145760586e28c8d45e11424653912 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=17d958bc6c8ac8260c2b18d5e6e033bb 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=0bdb3a92e0e7721921d77288c5594fd4 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/visual-indicator-mcp-tools.png?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=5eabd994c742cc7379e99b908e9dea3e 2500w" />
</Frame>

After clicking on the slider icon, you should see two tools listed:

<Frame>
  <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=040f7b6ec62f24b8fa0aaf4c5cab2d29" data-og-width="638" width="638" data-og-height="240" height="240" data-path="images/available-mcp-tools.png" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=588fae5b4ba6c310a527e91eb20d83bf 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=d277a424f541ff9a901072a4103ff05e 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=d12c286d06565cd45e175ea08c59fcf4 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=e30982d8fea169d8559777facd1a07e3 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=d90eaa83029d8ae66dec21d632c118b5 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/available-mcp-tools.png?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=f89e9bb7244323d208cf367bcd45f4ef 2500w" />
</Frame>

If your server isn't being picked up by Claude for Desktop, proceed to the [Troubleshooting](#troubleshooting) section for debugging tips.

If the tool settings icon has shown up, you can now test your server by running the following commands in Claude for Desktop:

* What's the weather in Sacramento?
* What are the active weather alerts in Texas?

<Frame>
  <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=dce7b2f8a06c20ba358e4bd2e75fa4c7" data-og-width="2780" width="2780" data-og-height="1849" height="1849" data-path="images/current-weather.png" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=bbb19f34c5df59f66bc6bbb75d2bc5ed 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=2392d7e765b897c5b78f9f53d41439d4 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=dc349e75341b046d35a649762774da49 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=deeb99214d9383ee4a0c8aaacb120049 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=5c6f948059635e376deeadce3893e9b9 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/current-weather.png?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=3922160478785cc88d5e98d418e8f7dd 2500w" />
</Frame>

<Frame>
  <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=c4762bf2bd84a8781846d2965af3e4a4" data-og-width="2809" width="2809" data-og-height="1850" height="1850" data-path="images/weather-alerts.png" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=e25afdd84f6ae9c612b898c6eb9c518d 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=1e7ef678cbc93c0966789e61d5209092 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=4dbaeb8840a7b1aeb73b188804877d71 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=68f5e0cb428c8b9cb53d28ec1108073b 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=56025243c2b8c6413f8da087122e848d 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/weather-alerts.png?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=12f50039e4a1c9544a22a9bdae46f719 2500w" />
</Frame>

<Note>
  Since this is the US National Weather service, the queries will only work for US locations.
</Note>

## What's happening under the hood

When you ask a question:

1. The client sends your question to Claude
2. Claude analyzes the available tools and decides which one(s) to use
3. The client executes the chosen tool(s) through the MCP server
4. The results are sent back to Claude
5. Claude formulates a natural language response
6. The response is displayed to you!

## Troubleshooting

<AccordionGroup>
  <Accordion title="Claude for Desktop Integration Issues">
    **Getting logs from Claude for Desktop**

    Claude.app logging related to MCP is written to log files in `~/Library/Logs/Claude`:

    * `mcp.log` will contain general logging about MCP connections and connection failures.
    * Files named `mcp-server-SERVERNAME.log` will contain error (stderr) logging from the named server.

    You can run the following command to list recent logs and follow along with any new ones:

    ```bash  theme={null}
    # Check Claude's logs for errors
    tail -n 20 -f ~/Library/Logs/Claude/mcp*.log
    ```

    **Server not showing up in Claude**

    1. Check your `claude_desktop_config.json` file syntax
    2. Make sure the path to your project is absolute and not relative
    3. Restart Claude for Desktop completely

    <Warning>
      To properly restart Claude for Desktop, you must fully quit the application:

      * **Windows**: Right-click the Claude icon in the system tray (which may be hidden in the "hidden icons" menu) and select "Quit" or "Exit".
      * **macOS**: Use Cmd+Q or select "Quit Claude" from the menu bar.

      Simply closing the window does not fully quit the application, and your MCP server configuration changes will not take effect.
    </Warning>

    **Tool calls failing silently**

    If Claude attempts to use the tools but they fail:

    1. Check Claude's logs for errors
    2. Verify your server builds and runs without errors
    3. Try restarting Claude for Desktop

    **None of this is working. What do I do?**

    Please refer to our [debugging guide](/legacy/tools/debugging) for better debugging tools and more detailed guidance.
  </Accordion>

  <Accordion title="Weather API Issues">
    **Error: Failed to retrieve grid point data**

    This usually means either:

    1. The coordinates are outside the US
    2. The NWS API is having issues
    3. You're being rate limited

    Fix:

    * Verify you're using US coordinates
    * Add a small delay between requests
    * Check the NWS API status page

    **Error: No active alerts for \[STATE]**

    This isn't an error - it just means there are no current weather alerts for that state. Try a different state or check during severe weather.
  </Accordion>
</AccordionGroup>

<Note>
  For more advanced troubleshooting, check out our guide on [Debugging MCP](/legacy/tools/debugging)
</Note>

## Next steps

<CardGroup cols={2}>
  <Card title="Building a client" icon="outlet" href="/docs/develop/build-client">
    Learn how to build your own MCP client that can connect to your server
  </Card>

  <Card title="Example servers" icon="grid" href="/examples">
    Check out our gallery of official MCP servers and implementations
  </Card>

  <Card title="Debugging Guide" icon="bug" href="/legacy/tools/debugging">
    Learn how to effectively debug MCP servers and integrations
  </Card>

  <Card title="Building MCP with LLMs" icon="comments" href="/tutorials/building-mcp-with-llms">
    Learn how to use LLMs like Claude to speed up your MCP development
  </Card>
</CardGroup>


# Debugging

> A comprehensive guide to debugging Model Context Protocol (MCP) integrations

Effective debugging is essential when developing MCP servers or integrating them with applications. This guide covers the debugging tools and approaches available in the MCP ecosystem.

<Info>
  This guide is for macOS. Guides for other platforms are coming soon.
</Info>

## Debugging tools overview

MCP provides several tools for debugging at different levels:

1. **MCP Inspector**
   * Interactive debugging interface
   * Direct server testing
   * See the [Inspector guide](/docs/tools/inspector) for details

2. **Claude Desktop Developer Tools**
   * Integration testing
   * Log collection
   * Chrome DevTools integration

3. **Server Logging**
   * Custom logging implementations
   * Error tracking
   * Performance monitoring

## Debugging in Claude Desktop

### Checking server status

The Claude.app interface provides basic server status information:

1. Click the <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=6dbffe2b165581db1a15fa563e731587" style={{display: 'inline', margin: 0, height: '1.3em'}} data-og-width="32" width="32" data-og-height="32" height="32" data-path="images/claude-desktop-mcp-plug-icon.svg" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=52542b77064250c772fa35d061f3ef76 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=25616acb73de37d56ed2cd64a65e312a 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=6f171c43990ee54f940a1c80d276fdca 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=f27d7296aa6996138d4ca44ce9cae6c4 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=62f7664fb6bac3b0ef523518cb60f65a 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-plug-icon.svg?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=c4ccfd0a249a60fbb03ac3d1925bdea8 2500w" /> icon to view:
   * Connected servers
   * Available prompts and resources

2. Click the "Search and tools" <img src="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=2742ec3fb97067e8591e68546c90221e" style={{display: 'inline', margin: 0, height: '1.3em'}} data-og-width="24" width="24" data-og-height="24" height="24" data-path="images/claude-desktop-mcp-slider.svg" data-optimize="true" data-opv="3" srcset="https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=280&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=52839f8519f476623c4fb5bb87ee24bd 280w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=560&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=f0491976e108286441fc6554309c5c4f 560w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=840&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=08e83eb102eda755a7db1eb27d16ebff 840w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=1100&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=2524a80752928b0206e68e8e1890d1aa 1100w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=1650&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=3c0dc88dadad5ed8e8af316965d00e0b 1650w, https://mintcdn.com/mcp/4ZXF1PrDkEaJvXpn/images/claude-desktop-mcp-slider.svg?w=2500&fit=max&auto=format&n=4ZXF1PrDkEaJvXpn&q=85&s=702363a955a631c40c342f9557d5cfdd 2500w" /> icon to view:
   * Tools made available to the model

### Viewing logs

Review detailed MCP logs from Claude Desktop:

```bash  theme={null}
# Follow logs in real-time
tail -n 20 -F ~/Library/Logs/Claude/mcp*.log
```

The logs capture:

* Server connection events
* Configuration issues
* Runtime errors
* Message exchanges

### Using Chrome DevTools

Access Chrome's developer tools inside Claude Desktop to investigate client-side errors:

1. Create a `developer_settings.json` file with `allowDevTools` set to true:

```bash  theme={null}
echo '{"allowDevTools": true}' > ~/Library/Application\ Support/Claude/developer_settings.json
```

2. Open DevTools: `Command-Option-Shift-i`

Note: You'll see two DevTools windows:

* Main content window
* App title bar window

Use the Console panel to inspect client-side errors.

Use the Network panel to inspect:

* Message payloads
* Connection timing

## Common issues

### Working directory

When using MCP servers with Claude Desktop:

* The working directory for servers launched via `claude_desktop_config.json` may be undefined (like `/` on macOS) since Claude Desktop could be started from anywhere
* Always use absolute paths in your configuration and `.env` files to ensure reliable operation
* For testing servers directly via command line, the working directory will be where you run the command

For example in `claude_desktop_config.json`, use:

```json  theme={null}
{
  "command": "npx",
  "args": [
    "-y",
    "@modelcontextprotocol/server-filesystem",
    "/Users/username/data"
  ]
}
```

Instead of relative paths like `./data`

### Environment variables

MCP servers inherit only a subset of environment variables automatically, like `USER`, `HOME`, and `PATH`.

To override the default variables or provide your own, you can specify an `env` key in `claude_desktop_config.json`:

```json  theme={null}
{
  "myserver": {
    "command": "mcp-server-myapp",
    "env": {
      "MYAPP_API_KEY": "some_key"
    }
}
}
```

### Concurrent AWS SSO logins (Claude Desktop)

When Claude Desktop launches, it starts all configured MCP servers at the same time. If your servers need AWS credentials and the current session has expired, each one may try to run `aws sso login` at startup. That can open multiple browser windows and often leads to confusing failures.

To prevent this, DS‑MCP’s startup scripts now serialize AWS SSO login per profile:

- Only one server initiates the browser login (per `AWS_PROFILE`).
- Other servers wait until credentials become valid, then continue automatically.
- A lightweight filesystem lock at `${XDG_CACHE_HOME:-$HOME/.cache}/ds-mcp/locks/aws-sso-<profile>.lock` coordinates this.

Tips:
- Pre‑authenticate once before launching Claude: `aws sso login --profile 3VDEV` (or your profile).
- Ensure `aws` CLI is installed and on `PATH` in the Claude process environment.
- You can set `AWS_PROFILE` per server in `claude_desktop_config.json` under each server’s `env` block.

### Server initialization

Common initialization problems:

1. **Path Issues**
   * Incorrect server executable path
   * Missing required files
   * Permission problems
   * Try using an absolute path for `command`

2. **Configuration Errors**
   * Invalid JSON syntax
   * Missing required fields
   * Type mismatches

3. **Environment Problems**
   * Missing environment variables
   * Incorrect variable values
   * Permission restrictions

### Connection problems

When servers fail to connect:

1. Check Claude Desktop logs
2. Verify server process is running
3. Test standalone with [Inspector](/docs/tools/inspector)
4. Verify protocol compatibility

## Implementing logging

### Server-side logging

When building a server that uses the local stdio [transport](/legacy/concepts/transports), all messages logged to stderr (standard error) will be captured by the host application (e.g., Claude Desktop) automatically.

<Warning>
  Local MCP servers should not log messages to stdout (standard out), as this will interfere with protocol operation.
</Warning>

For all [transports](/legacy/concepts/transports), you can also provide logging to the client by sending a log message notification:

<CodeGroup>
  ```python Python theme={null}
  server.request_context.session.send_log_message(
    level="info",
    data="Server started successfully",
  )
  ```

  ```typescript TypeScript theme={null}
  server.sendLoggingMessage({
    level: "info",
    data: "Server started successfully",
  });
  ```
</CodeGroup>

Important events to log:

* Initialization steps
* Resource access
* Tool execution
* Error conditions
* Performance metrics

### Client-side logging

In client applications:

1. Enable debug logging
2. Monitor network traffic
3. Track message exchanges
4. Record error states

## Debugging workflow

### Development cycle

1. Initial Development
   * Use [Inspector](/legacy/tools/inspector) for basic testing
   * Implement core functionality
   * Add logging points

2. Integration Testing
   * Test in Claude Desktop
   * Monitor logs
   * Check error handling

### Testing changes

To test changes efficiently:

* **Configuration changes**: Restart Claude Desktop
* **Server code changes**: Use Command-R to reload
* **Quick iteration**: Use [Inspector](/legacy/tools/inspector) during development

## Best practices

### Logging strategy

1. **Structured Logging**
   * Use consistent formats
   * Include context
   * Add timestamps
   * Track request IDs

2. **Error Handling**
   * Log stack traces
   * Include error context
   * Track error patterns
   * Monitor recovery

3. **Performance Tracking**
   * Log operation timing
   * Monitor resource usage
   * Track message sizes
   * Measure latency

### Security considerations

When debugging:

1. **Sensitive Data**
   * Sanitize logs
   * Protect credentials
   * Mask personal information

2. **Access Control**
   * Verify permissions
   * Check authentication
   * Monitor access patterns

## Getting help

When encountering issues:

1. **First Steps**
   * Check server logs
   * Test with [Inspector](/legacy/tools/inspector)
   * Review configuration
   * Verify environment

2. **Support Channels**
   * GitHub issues
   * GitHub discussions

3. **Providing Information**
   * Log excerpts
   * Configuration files
   * Steps to reproduce
   * Environment details

## Next steps

<CardGroup cols={2}>
  <Card title="MCP Inspector" icon="magnifying-glass" href="/legacy/tools/inspector">
    Learn to use the MCP Inspector
  </Card>
</CardGroup>
