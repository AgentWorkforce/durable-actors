import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const root = new URL("../../", import.meta.url)
const read = path => readFileSync(new URL(path, root), "utf8")

test("release images use the established Terse Artifact Registry", () => {
    const workflow = read(".github/workflows/release.yml")

    assert.match(workflow, /REGISTRY: us-central1-docker\.pkg\.dev/)
    assert.match(workflow, /IMAGE: us-central1-docker\.pkg\.dev\/fluid-analogy-473415-c2\/public\/durable-actors/)
    assert.match(workflow, /google-github-actions\/auth@/)
    assert.match(workflow, /actions\/attest@/)
    assert.doesNotMatch(workflow, /push-to-registry: true/)
})

test("npm publishes the downloaded tarball as a filesystem path", () => {
    const workflow = read(".github/workflows/release.yml")

    assert.match(workflow, /npm publish \.\/dist-tarballs\/durable-actors-\$RELEASE_VERSION\.tgz --access public/)
})

test("release publishes only after main CI passes for the tagged commit instead of rerunning tests", () => {
    for (const job of ["native-publish", "native-intel-publish", "image-push", "image", "npm", "crate", "python", "python-runtime", "python-runtime-intel"])
        assert.ok(dependsOn(job, "main-ci"), `${job} must wait for main CI`)
    assert.match(releaseJob("main-ci"), /gh run list .*--workflow ci\.yml --branch main --event push --commit "\$GITHUB_SHA"[\s\S]*gh run watch "\$run" .*--exit-status/)
    assert.doesNotMatch(
        read(".github/workflows/release.yml"),
        /uses: \.\/\.github\/workflows\/python\.yml|bun run --bun --cwd sdk (?:test|package:check)|cargo test --locked (?:--no-fail-fast|-- --ignored)/
    )
    assert.match(releaseJob("python-package"), /uv build --no-sources[\s\S]*name: python-package/)
    assert.ok(dependsOn("python", "python-package"))
})

test("macOS Intel publishes on its own path that primary publishing never waits for", () => {
    assert.deepEqual(nativeRunners("native-publish"), nativeRunners("native"))
    assert.ok(!nativeRunners("native").includes("macos-15-intel"))
    for (const job of ["native-intel", "native-intel-publish"]) assert.deepEqual(nativeRunners(job), ["macos-15-intel"])
    assert.ok(dependsOn("python-runtime-intel", "native-intel-publish"))
    for (const job of ["native-publish", "npm", "crate", "python-runtime", "python", "helm"]) assert.ok(!dependsOn(job, "native-intel"), `${job} must not wait for macOS Intel`)
})

test("primary Python wheels cover every primary runner and leave Intel to its own job", () => {
    const pattern = releaseJob("python-runtime").match(/pattern: (\S+)/)[1]
    const included = runner => new RegExp(`^${pattern.replaceAll("*", ".*")}$`).test(`python-runtime-${runner}`)
    assert.ok(nativeRunners("native").every(included), `${pattern} must match every primary runner`)
    assert.ok(!nativeRunners("native-intel").some(included), `${pattern} must exclude Intel`)
    assert.match(releaseJob("python-runtime-intel"), /name: python-runtime-macos-15-intel/)
})

test("CI tests pull requests and main while only releases build native runtimes and images", () => {
    const ci = read(".github/workflows/ci.yml")
    assert.doesNotMatch(ci, /native-build\.yml|build-push-action/)
    assert.match(ci, /if: github\.event_name == 'push'\n\s+run: cargo test --locked -- --ignored/)
    assert.match(releaseJob("native"), /uses: \.\/\.github\/workflows\/native-build\.yml/)
    assert.match(releaseJob("image-build"), /build-push-action/)
    assert.match(ci, /bun run --bun --cwd sdk package:check/)
})

test("pull requests test the newest Python and still build the SDK package", () => {
    const python = read(".github/workflows/python.yml")
    const [pullRequest, full] = python
        .match(/github\.event_name == 'pull_request' && '(.+?)' \|\| '(.+?)'/)
        .slice(1)
        .map(versions => JSON.parse(versions))
    assert.deepEqual(pullRequest, [full.at(-1)])
    assert.match(python, new RegExp(`name: Build distributions\\n\\s+if: matrix\\.python == '${full.at(-1)}'`))
})

test("main CI exercises direct host sockets with the built SDK", () => {
    assert.match(ciJob("rust"), /bun run --bun --cwd sdk build[\s\S]*cargo test --locked -- --ignored/)
})

test("Rust tests keep their own Cargo cache because Python jobs build without test features", () => {
    const python = read(".github/workflows/python.yml")
    const cacheKey = workflow => workflow.match(/shared-key: (.+)/)[1]
    assert.notEqual(cacheKey(ciJob("rust")), cacheKey(python))
    assert.match(python, /save-if: \$\{\{ github\.ref == 'refs\/heads\/main' && matrix\.python == '[\d.]+' \}\}/)
})

test("the SDK is packed once and reused by native tests and npm", () => {
    const packing = releaseJob("npm-package")
    assert.match(packing, /oven-sh\/setup-bun@v2/)
    assert.match(packing, /bun run --bun --cwd sdk build[\s\S]*bun pm --cwd sdk pack --ignore-scripts/)
    assert.match(packing, /actions\/upload-artifact@[\s\S]*name: sdk-package/)
    assert.equal(packing.match(/bun pm --cwd sdk pack --ignore-scripts/g)?.length, 1)
    for (const job of ["native-publish", "native-intel-publish", "npm"]) {
        assert.match(releaseJob(job), /actions\/download-artifact@[\s\S]*name: sdk-package/)
        assert.doesNotMatch(releaseJob(job), /bun (?:run --bun --cwd sdk build|pm --cwd sdk pack)/)
    }
    assert.match(releaseJob("native-publish"), /DURABLE_ACTORS_TEST_PACKAGE:[\s\S]*examples\/chat build/)
})

test("release publishes the observer dependency before the SDK", () => {
    const workflow = read(".github/workflows/release.yml")
    const npmJob = workflow.split("    npm:\n")[1].split("    crate:\n")[0]
    assert.equal(releaseJob("npm-package").match(/bun pm --cwd packages\/observer-ui pack --ignore-scripts/g)?.length, 1)
    assert.doesNotMatch(npmJob, /bun pm .* pack/)
    assert.match(npmJob, /npm publish \.\/dist-tarballs\/durable-actors-observer-.*\.tgz --access public[\s\S]*npm publish \.\/dist-tarballs\/durable-actors-.*\.tgz --access public/)
})

test("native package validation resolves the observer from the tested tarball", () => {
    for (const name of ["native-publish", "native-intel-publish"]) {
        const job = releaseJob(name)
        assert.match(job, /version: \$\{\{ needs\.preflight\.outputs\.version \}\}/)
        assert.match(job, /DURABLE_ACTORS_TEST_OBSERVER_PACKAGE:.*\/\.artifacts\/durable-actors-observer-\$\{\{ inputs\.version \}\}\.tgz/)
        assert.match(job, /bun pm pkg set "overrides\.durable-actors-observer=file:\$DURABLE_ACTORS_TEST_OBSERVER_PACKAGE"[\s\S]*bun add --cwd examples\/chat --ignore-scripts/)
    }
})

test("native and image builds start independently of validation and stage artifacts", () => {
    for (const job of ["native", "image-build"]) {
        assert.deepEqual(dependencies(job), ["preflight"])
        assert.match(releaseJob(job), /actions\/upload-artifact@/)
        assert.doesNotMatch(releaseJob(job), /push: true|gh release upload|docker push/)
    }
    assert.match(releaseJob("native"), /bun scripts\/build-runtime.mjs/)
    assert.match(releaseJob("image-build"), /outputs: type=docker,dest=/)
    assert.ok(dependsOn("native-publish", "native"))
    assert.ok(dependsOn("image-push", "image-build"))
})

test("Cargo caches are restored after toolchain selection", () => {
    for (const job of ["crate-check", "native"]) assert.match(releaseJob(job), /rustup default 1\.91\.0[\s\S]*Swatinem\/rust-cache@/)
    assert.match(read(".github/workflows/native-build.yml"), /shared-key: native-\$\{\{ matrix.runner \}\}/)
    const dockerfile = read("Dockerfile")
    assert.match(dockerfile, /--mount=type=cache,target=\/usr\/local\/cargo/)
    assert.match(dockerfile, /--mount=type=cache,target=\/build\/target/)
    assert.match(dockerfile, /cp target\/release\/durable-actors target\/release\/durable-actors-host \/out\//)
    assert.match(dockerfile, /COPY --from=builder \/out\/durable-actors/)
})

test("crate publication reuses successful verification without waiting for native or image builds", () => {
    assert.match(releaseJob("crate-check"), /cargo publish --locked --dry-run/)
    assert.ok(dependsOn("crate", "crate-check"))
    assert.match(releaseJob("crate"), /cargo publish --locked --no-verify/)
    for (const job of ["npm", "native", "native-publish", "image-build", "image"]) assert.ok(!dependsOn("crate", job), `crate must not wait for ${job}`)
})

test("release validates and publishes the pinned LTX crate before packaging the runtime", () => {
    assert.ok(dependsOn("crate-check", "ltx"))
    assert.match(releaseJob("preflight"), /tomllib[\s\S]*dependencies[\s\S]*"ltx"/)
    const job = releaseJob("ltx")
    assert.match(job, /repository: TerseAI\/terse-ltx/)
    assert.match(job, /ref: \$\{\{ needs\.preflight\.outputs\.ltx-revision \}\}/)
    assert.match(job, /persist-credentials: false/)
    assert.match(job, /cargo test --locked -- --include-ignored[\s\S]*cargo publish --locked --dry-run[\s\S]*name: Publish terse-ltx/)
    assert.equal(job.match(/CARGO_REGISTRY_TOKEN:/g)?.length, 1)
    assert.match(job, /name: Publish terse-ltx[\s\S]*CARGO_REGISTRY_TOKEN:[\s\S]*cargo publish --locked --no-verify/)
})

function ciJob(name) {
    return read(".github/workflows/ci.yml")
        .split(`    ${name}:\n`)[1]
        .split(/\n    [a-z-]+:\n/)[0]
}

function releaseJob(name) {
    const body = read(".github/workflows/release.yml").split(`    ${name}:\n`)[1]
    assert.ok(body, `Missing release job: ${name}`)
    const job = body.split(/\n    [a-z-]+:\n/)[0]
    const reusable = job.match(/^        uses: \.\/(.+)$/m)?.[1]
    return reusable ? `${job}\n${read(reusable)}` : job
}

function dependencies(name) {
    return (
        releaseJob(name)
            .match(/^        needs: (.+)$/m)?.[1]
            .replace(/[\[\]]/g, "")
            .split(/,\s*/) ?? []
    )
}

function dependsOn(job, dependency) {
    return dependencies(job).some(name => name === dependency || dependsOn(name, dependency))
}

function nativeRunners(name) {
    const job = releaseJob(name)
    const runners = job.match(/^            runners: '(.+)'$/m)?.[1] ?? job.match(/^                default: '(.+)'$/m)?.[1]
    return JSON.parse(runners)
}
