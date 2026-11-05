import { copyFile, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "node:path"

const { version } = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"))

for (const template of ["actor", "chat", "ai-chat", "documents"]) await buildTemplate(template)
await cp(
    new URL("../../skills/durable-actors-quickstart/", import.meta.url),
    new URL("../dist/skills/durable-actors-quickstart/", import.meta.url),
    { recursive: true }
)

async function buildTemplate(template) {
    const source = new URL(
        template === "actor" ? `../templates/${template}/` : `../../examples/${template}/`,
        import.meta.url
    )
    const destination = new URL(`../dist/templates/${template}/`, import.meta.url)
    await rm(destination, { recursive: true, force: true })
    await mkdir(destination, { recursive: true })
    const files = ["package.json", "tsconfig.json", "README.md", "src"]
    if (template !== "actor") files.push("index.html", ".env.example")
    for (const file of files)
        await cp(new URL(file, source), new URL(file, destination), {
            recursive: true,
            filter: file => !["generated", "node_modules", ".durable-actors", "dist"].includes(path.basename(file))
        })
    // Package archives omit .gitignore and bunfig.toml; init restores their names.
    await copyFile(new URL(".gitignore", source), new URL("gitignore", destination))
    await copyFile(new URL("../../bunfig.toml", import.meta.url), new URL("bunfig", destination))
    const metadata = JSON.parse(await readFile(new URL("package.json", destination), "utf8"))
    metadata.dependencies["durable-actors"] = version
    await writeFile(new URL("package.json", destination), JSON.stringify(metadata, null, 4) + "\n")
}
