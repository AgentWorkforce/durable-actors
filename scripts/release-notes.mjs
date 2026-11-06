import { execFileSync } from "node:child_process"
import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function formatReleaseNotes(body) {
    const sections = { Features: [], Fixes: [] }
    let section
    for (const line of body.split(/\r?\n/)) {
        const heading = /^#{2,3} (.+)$/.exec(line)
        if (heading) section = heading[1]
        const bullet = /^[*-] (.+)$/.exec(line)
        if (!bullet || section === "New Contributors") continue
        if (!Object.hasOwn(sections, section)) throw new Error(`Classify this PR with release:feature, release:fix, or release:skip before releasing: ${bullet[1]}`)
        sections[section].push(releaseBullet(bullet[1]))
    }
    return (
        Object.entries(sections)
            .map(([title, lines]) => [`## ${title}`, lines.join("\n")].filter(Boolean).join("\n\n"))
            .join("\n\n") + "\n"
    )
}

export function notesTarget({ head, releases }) {
    if (!/^[0-9a-f]{40}$/.test(head)) throw new Error("origin/main did not resolve to a commit.")
    return { previous: latestStableTag(releases), tag: head, sha: head }
}

function latestStableTag(releases) {
    const stable = releases.filter(release => !release.draft && !release.prerelease && /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(release.tag_name))
    stable.sort((left, right) => compareVersions(right.tag_name.slice(1), left.tag_name.slice(1)))
    const tag = stable[0]?.tag_name
    if (!tag) throw new Error("No stable release is published.")
    return tag
}

function compareVersions(left, right) {
    const [leftParts, rightParts] = [left, right].map(version => version.split(".").map(BigInt))
    for (let index = 0; index < 3; index++) if (leftParts[index] !== rightParts[index]) return leftParts[index] > rightParts[index] ? 1 : -1
    return 0
}

function releaseBullet(change) {
    const match = /^(.+) by @\S+ in (https:\/\/github\.com\/TerseAI\/durable-actors\/pull\/([1-9]\d*))$/.exec(change)
    if (!match) throw new Error(`Could not read the PR link from this generated change: ${change}`)
    return `- ${match[1]} ([#${match[3]}](${match[2]}))`
}

function mainFromMain() {
    execFileSync("git", ["fetch", "--quiet", "origin", "main"], { timeout: 60000 })
    const head = execFileSync("git", ["rev-parse", "origin/main"], { encoding: "utf8", timeout: 60000 }).trim()
    const releases = JSON.parse(execFileSync("gh", ["api", "repos/TerseAI/durable-actors/releases", "--paginate", "--slurp"], { encoding: "utf8", timeout: 60000, maxBuffer: 64 * 1024 * 1024 })).flat()
    const target = notesTarget({ head, releases })
    process.stdout.write(writeNotes(target.previous, target.tag, target.sha))
}

function main() {
    const [previous, tag, sha, ...extra] = process.argv.slice(2)
    if (!previous && !tag && !sha) return mainFromMain()
    if (!previous || !tag || !sha || extra.length) throw new Error("Usage: node scripts/release-notes.mjs [PREVIOUS_TAG NEW_TAG TARGET_SHA]")
    process.stdout.write(writeNotes(previous, tag, sha))
}

function writeNotes(previous, tag, sha) {
    const generated = execFileSync(
        "gh",
        [
            "api",
            "repos/TerseAI/durable-actors/releases/generate-notes",
            "-f",
            `tag_name=${tag}`,
            "-f",
            `target_commitish=${sha}`,
            "-f",
            `previous_tag_name=${previous}`,
            "-f",
            "configuration_file_path=.github/release.yml"
        ],
        { encoding: "utf8", timeout: 60000 }
    )
    return formatReleaseNotes(JSON.parse(generated).body)
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        main()
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
