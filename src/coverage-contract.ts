
import * as fs from 'fs';
import * as nats from 'nats';
import * as path from 'path';
import { Context, Contract } from 'fabric-contract-api';
import merge from 'ts-deepmerge';
import { R4 } from '@ahryman40k/ts-fhir-types';
import { either as E } from 'fp-ts';

export class CoverageContract extends Contract {
    nats_client = null;

    public async initLedger(ctx: Context) {
        console.info('============= START : Initialize Ledger ===========');
        console.info('============= END : Initialize Ledger ===========');
    }

    // GET resource by id and type
    public async queryResource(ctx: Context, id: string, type: string): Promise<string> {
        console.info('============= START : queryResource ===========');
        // form composite key
        const key = ctx.stub.createCompositeKey(type, [id]);

        const resourceBytes = await ctx.stub.getState(key);
        if (!resourceBytes || resourceBytes.length === 0) {
            throw new Error(`Resource of type ${type} with id '${id}' does not exist`);
        }
        console.log(resourceBytes.toString());
        console.info('============= END : queryResource ===========');
        return resourceBytes.toString();
    }

    // POST resource
    public async addResource(ctx: Context, id: string, resourceStr: string) {
        console.info('============= START : addResource ===========');

        // determine the resource type and id
        const resource: any = JSON.parse(resourceStr);
        const resource_type: string = resource.resourceType;
        const resource_id: string = resource.id;
        console.info(`addResource: received id: ${resource_id} resource type: ${resource_type} json: ${JSON.stringify(resource)}`);

        // create a composite key
        console.info('Creating composite key');
        const key = ctx.stub.createCompositeKey(resource_type, [resource_id]);

        // does a resource of this type already exist? If so, replace.
        const resourceBytes = await ctx.stub.getState(key);
        if (resourceBytes && resourceBytes.length > 0) {
            console.info(`Resource with id '${key}' exists; replacing resource.`);
        }

        // validate the input resource
        this.validateResource(resource_type, resource);

        // store the input resource
        await ctx.stub.putState(key, Buffer.from(JSON.stringify(resource)));

        if (resource_type == 'CoverageEligibilityRequest') {
            console.info('Processing eligibility request');
            return await this.processCoverageEligibilityRequest(ctx, resource);
        }
        console.info('============= END : addResource ===========');
    }

    // PUT resource
    public async replaceResource(ctx: Context, id: string, resourceStr: string) {
        console.info('============= START : replaceResource ===========');

        // determine the resource type and id
        const resource: any = JSON.parse(resourceStr);
        const resource_type: string = resource.resourceType;
        const resource_id: string = resource.id;
        console.info(`replaceResource: received id: ${resource_id} resource type: ${resource_type} json: ${JSON.stringify(resource)}`);

        // create a composite key
        const key = ctx.stub.createCompositeKey(resource_type, [resource_id]);

        const resourceBytes = await ctx.stub.getState(key);
        if (!resourceBytes || resourceBytes.length === 0) {
            console.info(`Resource with id ${key} does not exist; adding resource.`);
        }

        // validate the input resource
        this.validateResource(resource_type, resource);

        // store the input resource
        await ctx.stub.putState(key, Buffer.from(JSON.stringify(resource)));
        console.info('============= END : replaceResource ===========');
    }

    // PATCH resource
    public async updateResource(ctx: Context, id: string, resourceStr: string) {
        console.info('============= START : updateResource ===========');

        // determine the resource type and id
        const resource: any = JSON.parse(resourceStr);
        const resource_type: string = resource.resourceType;
        const resource_id: string = resource.id;
        console.info(`replaceResource: received id: ${resource_id} resource type: ${resource_type} json: ${JSON.stringify(resource)}`);

        // create a composite key
        const key: string = ctx.stub.createCompositeKey(resource_type, [resource_id]);

        const resourceBytes = await ctx.stub.getState(key);
        if (!resourceBytes || resourceBytes.length === 0) {
            throw new Error(`Resource with id '${key}' does not exist`);
        }

        // validate the input resource
        this.validateResource(resource_type, resource);

        const existingResource: any = JSON.parse(resourceBytes.toString());
        const newResource: any = JSON.parse(resourceStr);
        console.info(`updateResource: id: '${key}' existingResource: ` + JSON.stringify(existingResource));
        console.info(`updateResource: id: '${key}' newResource: ` + JSON.stringify(newResource));

        // Merge the values from newPatient into existingPatient
        const result = merge(existingResource, newResource);
        console.info(`updateResource: id: '${key}' merged Patient: ` + JSON.stringify(result));

        await ctx.stub.putState(key, Buffer.from(JSON.stringify(result)));
        console.info('============= END : updateResource ===========');
    }

    /*
     * Validate an input FHIR resource & throw an exception if not valid.
     * 
     * Extend to support runtime validation for other FHIR types.
     */
    validateResource(resource_type: string, resource: any) {
        let validation_result;

        switch(resource_type) {
            case 'Coverage':
                validation_result = R4.RTTI_Coverage.decode(resource);
                break;
            case 'CoverageEligibilityRequest':
                validation_result = R4.RTTI_CoverageEligibilityRequest.decode(resource);
                break;
            case 'CoverageEligibilityResponse':
                validation_result = R4.RTTI_CoverageEligibilityResponse.decode(resource);
                break;
            case 'Organization':
                validation_result = R4.RTTI_Organization.decode(resource);
                break;
            case 'Patient':
                validation_result = R4.RTTI_Patient.decode(resource);
                break;
            default:
                throw new Error(`FHIR validation error: unsupported resource type: ${resource_type}`);
        }

        if (E.isLeft(validation_result) ) {
            console.info('validateResource: isLeft');
            throw new Error(`FHIR validation error: ${JSON.stringify(validation_result.left)}`);
        }
        
        if (E.isRight(validation_result) ) {
            console.info(`validateResource: Successful validation: ${JSON.stringify(validation_result.right)}`);
        }
    }

    /*
     * Process a CoverageEligibilityRequest & return the response.
     *
     * Does the patient have insurance with the insurer based on the coverage info provided?
     */
    async processCoverageEligibilityRequest(ctx: Context, resource: any): Promise<any> {
        let result: boolean = false;

        // Look up major references for validation
        const patient: any = await this.getStateFromReference(ctx, resource.patient.reference);
        const insurer: any = await this.getStateFromReference(ctx, resource.insurer.reference);
        const coverage: any = await this.getStateFromReference(ctx, resource.insurance[0].coverage.reference);
        console.info('Obtained patient, insurer and coverage objects');

        // At this point we've found the referenced objects; make sure they contain the correct results:
        // match the Coverage payor reference with the CoverageEligibilityRequest insurer.reference
        // match the Coverage subscriber patient reference with the CoverageEligibilityRequest patient.reference
        // match the Coverage period with the CoverageEligibilityRequest created date
        if (coverage.payor[0].reference == resource.insurer.reference &&
            coverage.subscriber.reference == resource.patient.reference &&
            this.dateInPeriod(resource.created, coverage.period.start, coverage.period.end)) {
            console.info('CoverageEligibilityResponse = true');
            result = true;
        } else {
            console.info('CoverageEligibilityResponse = false');
            result = false;
        }
        await this.sendCoverageEligibilityResponse(result, resource);
    }

    /*
     * For a given FHIR reference, get the object from world state.
     */
    async getStateFromReference(ctx: Context, reference: string): Promise<any> {
        // get the resource type and id
        const ref: Array<string> = reference.split('/');

        // create a composite key
        const key: string = ctx.stub.createCompositeKey(ref[0], [ref[1]]);

        // get the object from world state
        const resourceBytes = await ctx.stub.getState(key);
        if (!resourceBytes || resourceBytes.length === 0) {
            throw new Error(`getStateFromReference: Resource with id '${key}' does not exist`);
        }

        const resource: any = JSON.parse(resourceBytes.toString());
        return resource;
    }

    /*
     * Determine if a given date string is within a period identified by two date strings.
     */
    dateInPeriod(request_date: string, start_date: string, end_date: string): boolean {
        // Expected date format "2014-08-16"
        let request = new Date(request_date).valueOf();
        let start = new Date(start_date).valueOf();
        let end = new Date(end_date).valueOf();
        console.info(`dateInPeriod: request= ${request} start=${start} end=${end}`);

        return (request >= start && request <= end) ? true : false;
    }

    /*
     * Create the CoverageEligibilityResponse and hand off to NATS.
     */
    async sendCoverageEligibilityResponse(result: boolean, request: any) {

        let disposition: string = (result) ? "Policy is currently in effect." : "Policy is not in effect.";
        let today: string = new Date().toISOString().slice(0,10);

        let message: any = {
            "resourceType": "CoverageEligibilityResponse",
            "id": request.id,
            "text": {
              "status": "generated",
              "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">A human-readable rendering of the CoverageEligibilityResponse.</div>"
            },
            "identifier": [
              {
                "system": "http://localhost:5000/fhir/coverageeligibilityresponse/"+request.id,
                "value": request.id
              }
            ],
            "status": "active",
            "purpose": [
              "validation"
            ],
            "patient": {
              "reference": request.patient.reference
            },
            "created": today,
            "request": {
              "reference": "http://www.BenefitsInc.com/fhir/coverageeligibilityrequest/"+request.id
            },
            "outcome": "complete",
            "disposition": disposition,
            "insurer": {
              "reference": request.insurer.reference
            },
            "insurance": [
              {
                "coverage": {
                  "reference": request.insurer.reference
                },
                "inforce": result
              }
            ]
        };

        this.validateResource('CoverageEligibilityResponse', message);
        console.info('Validated CoverageEligibilityResponse');

        await this.sendNATSMessage('EVENTS.coverageeligibilityresponse', message);
        console.info('Sent CoverageEligibilityResponse via NATS');
    }

    /*
     * Create the NATS client.
     */
    async createNATSClient(): Promise<nats.JetStreamClient> {
        const nkey = fs.readFileSync(path.resolve(__dirname, '../conf/nats-server.nk'));
        let server: string = 'tls://nats-server:4222';
        
        let nc = await nats.connect({
            servers: server,
            authenticator: nats.nkeyAuthenticator(new TextEncoder().encode(nkey.toString())),
            tls: {
                caFile: path.resolve(__dirname, '../conf/lfh-root-ca.pem'),
            }
        });
        console.log(`Connected to NATS server ${server}`);

        // create a jetstream client:
        const js = nc.jetstream();
        return js;
    }

    /*
     * Send a message to the configured NATS server.
     */
    async sendNATSMessage(subject: string, message: any) {
        if (!this.nats_client) {
            this.nats_client = await this.createNATSClient();
        }
        console.log('Publishing NATS message');
        try {
            const headers = nats.headers();
            headers.append("Nats-Msg-Id", message.id);
            let pa = await this.nats_client.publish(subject, new TextEncoder().encode(JSON.stringify(message)), { headers });
            const stream = pa.stream;
            const seq = pa.seq;
            const duplicate = pa.duplicate;
            console.log(`Published NATS message to subject: ${subject} stream: ${stream} seq: ${seq} duplicate: ${duplicate}`);
        } catch (ex) {
            console.log(`Error publishing to JetStream stream: ${ex}`);
        }
    }
}
