import type { Readable } from 'stream';
import { DataFactory } from 'n3';
import type { Quad } from 'rdf-js';
import streamifyArray from 'streamify-array';
import { v4 as uuid } from 'uuid';
import type { Patch } from '../ldp/http/Patch';
import type { Representation } from '../ldp/representation/Representation';
import { RepresentationMetadata } from '../ldp/representation/RepresentationMetadata';
import type { ResourceIdentifier } from '../ldp/representation/ResourceIdentifier';
import { INTERNAL_QUADS } from '../util/ContentTypes';
import { ConflictHttpError } from '../util/errors/ConflictHttpError';
import { MethodNotAllowedHttpError } from '../util/errors/MethodNotAllowedHttpError';
import { NotFoundHttpError } from '../util/errors/NotFoundHttpError';
import { UnsupportedHttpError } from '../util/errors/UnsupportedHttpError';
import type { MetadataController } from '../util/MetadataController';
import { CONTENT_TYPE, HTTP, LDP, RDF } from '../util/UriConstants';
import { ensureTrailingSlash, trimTrailingSlashes } from '../util/Util';
import type { DataAccessor } from './accessors/DataAccessor';
import type { ContainerManager } from './ContainerManager';
import type { ResourceStore } from './ResourceStore';

/**
 * ResourceStore which uses a DataAccessor for backend access.
 *
 * This class is important because this way different stores don't all need to implement behaviour
 * Examples:
 *  * Converting container metadata to data
 *  * Converting slug to URI
 *  * Checking if addResource target is a container
 *  * Checking if no containment triples are written to a container
 *  * etc.
 *
 * Currently "metadata" is seen as something that is not directly accessible.
 * That means that a user can't write directly to the metadata of a resource, only indirectly through headers.
 * (Except for containers where data and metadata overlap).
 *
 * The one thing this store does not take care of (yet?) are containment triples for containers
 *
 * Work has been done to minimize the amount of required calls to the DataAccessor,
 * but the main disadvantage is that sometimes multiple calls are required where a specific store might only need one.
 */
export class DataAccessorBasedStore implements ResourceStore {
  private readonly accessor: DataAccessor;
  private readonly base: string;
  private readonly metadataController: MetadataController;
  private readonly containerManager: ContainerManager;

  public constructor(accessor: DataAccessor, base: string, metadataController: MetadataController,
    containerManager: ContainerManager) {
    this.accessor = accessor;
    this.base = ensureTrailingSlash(base);
    this.metadataController = metadataController;
    this.containerManager = containerManager;
  }

  public async getRepresentation(identifier: ResourceIdentifier): Promise<Representation> {
    this.validateIdentifier(identifier);

    // In the future we want to use getNormalizedMetadata and redirect in case the identifier differs
    const metadata = await this.accessor.getMetadata(identifier);

    if (this.isExistingContainer(metadata)) {
      metadata.contentType = INTERNAL_QUADS;
      const result = {
        binary: false,
        get data(): Readable {
          // This allows other modules to still add metadata before the output data is written
          return streamifyArray(result.metadata.quads());
        },
        metadata,
      };
      return result;
    }

    return { binary: metadata.contentType !== INTERNAL_QUADS, data: await this.accessor.getData(identifier), metadata };
  }

  public async addResource(container: ResourceIdentifier, representation: Representation): Promise<ResourceIdentifier> {
    this.validateIdentifier(container);
    await this.accessor.canHandle(representation);

    const isContainer = this.isNewContainer(representation.metadata);
    const slug = representation.metadata.get(HTTP.slug)?.value;
    let newID: ResourceIdentifier = this.createURI(container, isContainer, slug);
    representation.metadata.removeAll(HTTP.slug);

    // Using the parent metadata as we can also use that later to check if the nested containers maybe need to be made
    let parentMetadata: RepresentationMetadata | undefined;
    try {
      // Make sure we don't already have a resource with this exact name (or with differing trailing slash)
      parentMetadata = await this.accessor.getNormalizedMetadata(container);
      const withSlash = ensureTrailingSlash(newID.path);
      const withoutSlash = trimTrailingSlashes(newID.path);
      const exists = parentMetadata.getAll(LDP.contains).some((term): boolean =>
        term.value === withSlash || term.value === withoutSlash);
      if (exists) {
        newID = this.createURI(container, isContainer);
      }
    } catch (error: unknown) {
      if (!(error instanceof NotFoundHttpError)) {
        throw error;
      }

      // When a POST method request targets a non-container resource without an existing representation,
      // the server MUST respond with the 404 status code.
      if (!container.path.endsWith('/')) {
        throw new NotFoundHttpError();
      }
    }

    if (parentMetadata && !this.isExistingContainer(parentMetadata)) {
      throw new MethodNotAllowedHttpError('The given path is not a valid container.');
    }

    // Need at least 1 container if parent doesn't exist yet
    await this.writeData(newID, representation, isContainer, typeof parentMetadata !== 'object');

    return newID;
  }

  public async setRepresentation(identifier: ResourceIdentifier, representation: Representation): Promise<void> {
    this.validateIdentifier(identifier);
    await this.accessor.canHandle(representation);

    // Check if the resource already exists
    let oldMetadata: RepresentationMetadata | undefined;
    try {
      oldMetadata = await this.accessor.getNormalizedMetadata(identifier);

      // Might want to redirect in the future
      if (oldMetadata.identifier.value !== identifier.path) {
        throw new ConflictHttpError(`${identifier.path} conflicts with existing path ${oldMetadata.identifier.value}`);
      }
    } catch (error: unknown) {
      // Doesn't exist yet
      if (!(error instanceof NotFoundHttpError)) {
        throw error;
      }
    }

    const isContainer = this.isNewContainer(representation.metadata, identifier.path);
    if (oldMetadata && isContainer !== this.isExistingContainer(oldMetadata)) {
      throw new ConflictHttpError('Input resource type does not match existing resource type.');
    }
    if (isContainer !== identifier.path.endsWith('/')) {
      throw new UnsupportedHttpError('Containers should have a `/` at the end of their path, resources should not.');
    }

    // Potentially have to create containers if it didn't exist yet
    return this.writeData(identifier, representation, isContainer, typeof oldMetadata !== 'object');
  }

  public async modifyResource(identifier: ResourceIdentifier, patch: Patch): Promise<void> {
    this.validateIdentifier(identifier);
    return this.accessor.modifyResource(identifier, patch);
  }

  public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
    this.validateIdentifier(identifier);
    if (ensureTrailingSlash(identifier.path) === this.base) {
      throw new MethodNotAllowedHttpError('Cannot delete root container.');
    }
    const metadata = await this.accessor.getMetadata(identifier);
    if (metadata.getAll(LDP.contains).length > 0) {
      throw new ConflictHttpError('Can only delete empty containers.');
    }
    return this.accessor.deleteResource(identifier);
  }

  /**
   * Verify if the given identifier matches the stored base.
   */
  private validateIdentifier(identifier: ResourceIdentifier): void {
    if (!identifier.path.startsWith(this.base)) {
      throw new NotFoundHttpError();
    }
  }

  /**
   * Write the given resource to the DataAccessor. Metadata will be updated with necessary triples.
   * In case of containers `handleContainerData` will be used to verify the data.
   * @param identifier - Identifier of the resource.
   * @param representation - Corresponding Representation.
   * @param isContainer - Is the incoming resource a container?
   * @param createContainers - Should parent containers (potentially) be created?
   */
  private async writeData(identifier: ResourceIdentifier, representation: Representation, isContainer: boolean,
    createContainers?: boolean): Promise<void> {
    if (isContainer) {
      await this.handleContainerData(representation);
    }

    if (createContainers) {
      await this.createRecursiveContainers(await this.containerManager.getContainer(identifier));
    }

    const { metadata } = representation;
    metadata.identifier = DataFactory.namedNode(identifier.path);
    metadata.addQuads(this.metadataController.generateResourceQuads(metadata.identifier, isContainer));

    if (isContainer) {
      await this.accessor.writeContainer(identifier, representation.metadata);
    } else {
      await this.accessor.writeDataResource(identifier, representation.data, representation.metadata);
    }
  }

  /**
   * Verify if the incoming data for a container is valid (RDF and no containment triples).
   * Adds the container data to its metadata afterwards.
   *
   * @param representation - Container representation.
   */
  private async handleContainerData(representation: Representation): Promise<void> {
    let quads: Quad[];
    try {
      quads = await this.metadataController.parseQuads(representation.data);
    } catch (error: unknown) {
      // Make error more readable to know what happened
      if (error instanceof Error) {
        throw new UnsupportedHttpError(`Can only create containers with RDF data. ${error.message}`);
      }
      throw error;
    }

    // Make sure there are no containment triples in the body
    for (const quad of quads) {
      if (quad.predicate.value === LDP.contains) {
        throw new ConflictHttpError('Container bodies are not allowed to have containment triples.');
      }
    }

    // Input content type doesn't matter anymore
    representation.metadata.removeAll(CONTENT_TYPE);

    // Container data is stored in the metadata
    representation.metadata.addQuads(quads);
  }

  /**
   * Generates a new URI for a resource in the given container, potentially using the given slug.
   * @param container - Parent container of the new URI.
   * @param isContainer - Does the new URI represent a container?
   * @param slug - Slug to use for the new URI.
   */
  private createURI(container: ResourceIdentifier, isContainer: boolean, slug?: string): ResourceIdentifier {
    return { path:
        `${ensureTrailingSlash(container.path)}${slug ? trimTrailingSlashes(slug) : uuid()}${isContainer ? '/' : ''}` };
  }

  /**
   * Checks if the given metadata represents a (potential) container,
   * both based on the metadata and the URI.
   * @param metadata - Metadata of the (new) resource.
   * @param suffix - Suffix of the URI. Can be the full URI, but only the last part is required.
   */
  private isNewContainer(metadata: RepresentationMetadata, suffix?: string): boolean {
    let isContainer: boolean;
    try {
      isContainer = this.isExistingContainer(metadata);
    } catch {
      const slug = suffix ?? metadata.get(HTTP.slug)?.value;
      isContainer = Boolean(slug?.endsWith('/'));
    }
    return isContainer;
  }

  /**
   * Checks if the given metadata represents a container, purely based on metadata type triples.
   * Since type metadata always gets generated when writing resources this should never fail on stored resources.
   * @param metadata - Metadata to check.
   */
  private isExistingContainer(metadata: RepresentationMetadata): boolean {
    const types = metadata.getAll(RDF.type);
    if (types.length === 0) {
      throw new Error('Unknown resource type.');
    }
    return types.some((type): boolean => type.value === LDP.Container || type.value === LDP.BasicContainer);
  }

  /**
   * Create containers starting from the root until the given identifier corresponds to an existing container.
   * Will throw errors if the identifier of the last existing "container" corresponds to an existing data resource.
   * @param container - Identifier of the container which will need to exist.
   */
  private async createRecursiveContainers(container: ResourceIdentifier): Promise<void> {
    try {
      const metadata = await this.accessor.getNormalizedMetadata(container);
      if (!this.isExistingContainer(metadata)) {
        throw new ConflictHttpError(`Creating container ${container.path} conflicts with an existing resource.`);
      }
    } catch (error: unknown) {
      if (error instanceof NotFoundHttpError) {
        // Make sure the parent exists first
        await this.createRecursiveContainers(await this.containerManager.getContainer(container));
        await this.writeData(container, this.getEmptyContainerRepresentation(container), true);
      } else {
        throw error;
      }
    }
  }

  /**
   * Generates the minimal representation for an empty container.
   * @param container - Identifier of this new container.
   */
  private getEmptyContainerRepresentation(container: ResourceIdentifier): Representation {
    return {
      binary: true,
      data: streamifyArray([]),
      metadata: new RepresentationMetadata(container.path),
    };
  }
}
