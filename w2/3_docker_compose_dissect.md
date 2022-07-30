What are the `x-<tag>`,  `&<tag>` `<< : <tag>` syntax for?
they are 2 separate things, but they often come together.

- `x-<tag>` is docker's own syntex called **Extension fields**, basically tell YAML parser to ignore this section.
    people often put variable definitions or common section definitions here.

- `&<tag>` this is official YAML stuff called **Anchor**, allowing other sections to reference and include the content.
    use `*<tag>` to reference the anchor.

- << : <tag> another YAML stuff called **Merge type**, we can reference a anchor and override some value.


How passing environment variables to yaml file and container.

`.env` file only carry environment variables to yaml config file, to further pass the variable to container, we have to specify it in the `environment` section.